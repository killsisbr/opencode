import z from "zod"
import path from "path"
import { Tool } from "./tool"
import DESCRIPTION from "./verify.txt"
import { AppFileSystem } from "@/filesystem"
import { Instance } from "../project/instance"
import { Effect, Stream } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"

const Parameters = z.object({
  scriptPath: z
    .string()
    .describe("Path to the verification script. Defaults to './check.mjs' if not provided.")
    .optional(),
})

const parse = (text: string) => {
  const match = text.match(/TESTS\s+PASS=(\d+)\s+FAIL=(\d+)/i)
  if (!match) return { status: "ERROR" as const, passed: 0, failed: 0 }
  const passed = Number.parseInt(match[1], 10)
  const failed = Number.parseInt(match[2], 10)
  return { status: failed === 0 ? ("PASS" as const) : ("FAIL" as const), passed, failed }
}

export const VerifyTool = Tool.define(
  "verify",
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner
    const fs = yield* AppFileSystem.Service

    const run = Effect.fn("VerifyTool.run")(function* (
      input: { scriptPath: string; cwd: string },
      ctx: Tool.Context,
    ) {
      let output = ""
      let error = ""

      const script = path.basename(input.scriptPath)
      const ext = path.extname(script).toLowerCase()
      const js = ext === ".mjs" || ext === ".js" || ext === ".cjs"
      const command = js ? "node" : input.scriptPath
      const args = js ? [input.scriptPath] : []

      const code: number | null = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(
            ChildProcess.make(command, args, {
              cwd: input.cwd,
              stdin: "ignore",
            }),
          )

          yield* Effect.forkScoped(
            Stream.runForEach(Stream.decodeText(handle.stdout), (chunk) =>
              Effect.suspend(() => {
                output += chunk
                return ctx.metadata({ metadata: { output: output.slice(0, 200) } })
              }),
            ),
          )

          yield* Effect.forkScoped(
            Stream.runForEach(Stream.decodeText(handle.stderr), (chunk) =>
              Effect.sync(() => {
                error += chunk
              }),
            ),
          )

          return yield* handle.exitCode
        }),
      ).pipe(Effect.orDie)

      const result = parse(output)
      const body = (output.trim() || error.trim()).slice(0, 2000)
      const summary = result.status === "ERROR" && code !== 0 ? `${body}\n(exit ${code})` : body

      return {
        title: `${script}: ${result.status} ${result.passed}/${result.passed + result.failed}`,
        metadata: { output: summary.slice(0, 200) },
        output: summary || `No output from ${script}`,
      }
    })

    return () =>
      Effect.sync(() => ({
        description: DESCRIPTION,
        parameters: Parameters,
        execute: (params: z.infer<typeof Parameters>, ctx: Tool.Context) =>
          Effect.gen(function* () {
            const cwd = Instance.directory
            const scriptPath = params.scriptPath ?? "./check.mjs"
            const target = path.isAbsolute(scriptPath) ? scriptPath : path.join(cwd, scriptPath)

            if (!(yield* fs.existsSafe(target).pipe(Effect.orElseSucceed(() => false)))) {
              return {
                title: "Verification script not found",
                metadata: { output: target },
                output: `Verification script not found: ${scriptPath}`,
              }
            }

            return yield* run({ scriptPath: target, cwd }, ctx)
          }),
      }))
  }),
)
