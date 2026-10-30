# Compiles `nats_trim_safe.jl` with JuliaC `--trim=safe` and runs the executable
# against a JetStream server. The test fails on any trim verifier finding except
# those inside HTTP.jl's client WebSocket handshake (see
# `_trim_upstream_finding`).
#
# `HTTP.WebSockets.open(url)`, which `ws://` connections use, reads the handshake
# response with an unknown body type, which `juliac --trim` cannot compile. While
# those findings stop the `--trim=safe` build, the test builds the same code with
# `--trim=unsafe-warn`, which still produces the executable; the workload
# connects with `nats://`, so the executable never runs that code. Once HTTP.jl
# fixes it, the `--trim=safe` build produces the executable itself and
# `_trim_upstream_finding` can go.

# Julia 1.12's trim verifier cannot resolve JSON writes of fields whose `lower`
# tag returns `Union{JSON.Omit, T}`, which JetStream request configs use to leave
# out empty values.
const _TRIM_SUPPORTED = VERSION >= v"1.13.0-rc1"
const _TRIM_PRE_RELEASE = !isempty(VERSION.prerelease)
const _TRIM_SETUP_TIMEOUT_S = 600.0
const _TRIM_COMPILE_TIMEOUT_S = 900.0
const _TRIM_RUN_TIMEOUT_S = 120.0

# `Pkg.test` restricts JULIA_LOAD_PATH, which hides stdlibs such as Pkg from
# subprocesses.
function _trim_cmd(cmd::Cmd)
    env = Dict{String,String}(k => v for (k, v) in ENV if k != "JULIA_LOAD_PATH")
    return setenv(cmd, env)
end

function _trim_run(cmd::Cmd; timeout_s::Float64)
    output_path = tempname()
    exit_code = -1
    timed_out = false
    open(output_path, "w") do out
        proc = run(pipeline(ignorestatus(_trim_cmd(cmd)); stdout = out, stderr = out); wait = false)
        timed_out = timedwait(() -> !Base.process_running(proc), timeout_s; pollint = 0.5) == :timed_out
        timed_out && kill(proc)
        wait(proc)
        exit_code = something(proc.exitcode, -1)
    end
    output = read(output_path, String)
    rm(output_path; force = true)
    return exit_code, output, timed_out
end

_trim_print(label, output) = println("---- trim $label output ----\n", output, "\n---- end trim $label output ----")

# One block per verifier finding, starting at its "Error #N" or "Warning #N" line.
function _trim_findings(output::String)
    starts = [m.offset for m in eachmatch(r"(?:Verifier error|Verifier warning|Error|Warning) #\d+:", output)]
    stops = [starts[2:end]; ncodeunits(output) + 1]
    return [output[start:prevind(output, stop)] for (start, stop) in zip(starts, stops)]
end

# A finding is upstream when its innermost frame is in HTTP.jl's websocket code
# and the call came through `HTTP.WebSockets.open(url; ...)`.
function _trim_upstream_finding(block::AbstractString)
    innermost = match(r"Stacktrace:\s*\n\s*\[1\][^\n]*\n\s*@ ([^\n]*)", block)
    innermost === nothing && return false
    occursin("http_websockets.jl", innermost.captures[1]) || return false
    return occursin(r"\] open\(url::String[^\n]*\n\s*@ HTTP\.WebSockets ", block)
end

function _trim_compile_and_run(url::String)
    julia = joinpath(Sys.BINDIR, Base.julia_exename())
    nats_path = normpath(joinpath(@__DIR__, ".."))
    script = joinpath(@__DIR__, "nats_trim_safe.jl")
    mktempdir() do dir
        env = joinpath(dir, "env")
        setup = "import Pkg; Pkg.develop(path = $(repr(nats_path))); Pkg.add(\"JuliaC\")"
        exit_code, output, timed_out = _trim_run(`$julia --startup-file=no --history-file=no --project=$env -e $setup`;
            timeout_s = _TRIM_SETUP_TIMEOUT_S)
        @test !timed_out && exit_code == 0
        (timed_out || exit_code != 0) && return _trim_print("setup", output)

        juliac(mode) = `$julia --startup-file=no --history-file=no --code-coverage=none --project=$env
            -e "using JuliaC; JuliaC.main(ARGS)" -- --output-exe nats_trim --bundle $(joinpath(dir, mode))
            --project=$env --trim=$mode --experimental $script`
        exit_code, output, timed_out = _trim_run(juliac("safe"); timeout_s = _TRIM_COMPILE_TIMEOUT_S)
        findings = _trim_findings(output)
        unexpected = filter(!_trim_upstream_finding, findings)
        @test !timed_out
        @test isempty(unexpected)
        # Without findings the safe build must succeed; with only the upstream
        # ones it stops after verification.
        built = exit_code == 0 && isempty(findings)
        (timed_out || !isempty(unexpected) || (!built && isempty(findings))) && return _trim_print("compile", output)
        mode = "safe"
        if !built
            println("[trim] $(length(findings)) finding(s) in HTTP.jl's client WebSocket handshake; building with --trim=unsafe-warn to run the executable")
            mode = "unsafe-warn"
            exit_code, output, timed_out = _trim_run(juliac(mode); timeout_s = _TRIM_COMPILE_TIMEOUT_S)
            @test !timed_out && exit_code == 0
            (timed_out || exit_code != 0) && return _trim_print("compile", output)
        end

        executable = joinpath(dir, mode, "bin", Sys.iswindows() ? "nats_trim.exe" : "nats_trim")
        @test isfile(executable)
        isfile(executable) || return nothing
        exit_code, output, timed_out = _trim_run(`$executable $url`; timeout_s = _TRIM_RUN_TIMEOUT_S)
        @test !timed_out
        @test exit_code == 0
        @test occursin("NATS trim workload passed", output)
        (timed_out || exit_code != 0) && _trim_print("executable", output)
    end
    return nothing
end

@testset "Trim compile" begin
    if Sys.WORD_SIZE != 64 || Sys.iswindows()
        println("[trim] skip: the trim test runs on 64-bit Linux and macOS")
    elseif !_TRIM_SUPPORTED
        println("[trim] skip Julia < 1.13: see _TRIM_SUPPORTED")
    elseif _TRIM_PRE_RELEASE
        println("[trim] skip prerelease Julia: trim verifier behavior is not stable yet")
    else
        with_nats(_trim_compile_and_run)
    end
end
