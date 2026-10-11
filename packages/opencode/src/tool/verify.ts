import z from "zod"
import { Tool } from "./tool"
import { Log } from "@/util/log"
import { AppFileSystem } from "@/filesystem"
import { Effect, Stream } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import * as path from "path"

const Parameters = z.object({
  scriptPath: z
    .string()
    .describe(
      "Path to the verification script. Defaults to './check.mjs' if not provided."
    )
    .optional(),
})

export const VerifyTool = Tool.define(
  "verify",
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner
    const fs = yield* AppFileSystem.Service

    const run = Effect.fn("VerifyTool.run")(function* (
      input: {
        scriptPath: string
        cwd: string
      },
      ctx: Tool.Context,
    ) => {
      let output = ""
      let errorOutput = ""

      // Determine the command to run the script
      // We'll try to run with node if it's a .mjs or .js file, otherwise with bash
      const ext = path.extname(input.scriptPath).toLowerCase()
      let command: string
      let args: string[] = []

      if (ext === ".mjs" || ext === ".js") {
        command = "node"
        args = [input.scriptPath]
      } else {
        // Assume it's a shell script or executable
        command = input.scriptPath
        args = []
      }

      const code: number | null = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(
            command,
            args,
            {
              cwd: input.cwd,
              stdin: "ignore",
            }
          )

          // Capture stdout
          yield* Effect.forkScoped(
            Stream.runForEach(Stream.decodeText(handle.stdout), (chunk) => {
              output += chunk
              return ctx.metadata({
                metadata: {
                  output: output.slice(0, 200), // Truncate for metadata
                },
              })
            })
          )

          // Capture stderr
          yield* Effect.forkScoped(
            Stream.runForEach(Stream.decodeText(handle.stderr), (chunk) => {
              errorOutput += chunk
            })
          )

          const exit = yield* handle.exitCode
          return exit
        })
      )

      // Parse the output for the TESTS PASS line
      const passMatch = output.match(/TESTS\s+PASS=(\d+)\s+FAIL=(\d+)/i)
      let passed = 0
      let failed = 0
      let status: "PASS" | "FAIL" | "ERROR" = "ERROR"

      if (passMatch) {
        passed = parseInt(passMatch[1], 10)
        failed = parseInt(passMatch[2], 10)
        status = failed === 0 ? "PASS" : "FAIL"
      } else if (code !== null && code !== 0) {
        // If the script exited with non-zero and we didn't find the pattern, it's an error
        status = "ERROR"
      } else if (code === 0 && !passMatch) {
        // Script exited with 0 but no TESTS PASS line -> we treat as ERROR because we don't know the result
        status = "ERROR"
      }

      return {
        status,
        passed,
        failed,
        summary: output.trim().slice(0, 200), // Truncate summary to 200 chars
        // We don't leak ground truth, so we don't provide expected/actual unless the script does.
        // But the script might output EXPECTED and ACTUAL? We'll leave it open for the script to include in summary.
      }
    })

    return () =>
      Effect.sync(() => {
        const description = "Run a verification script and return the result."

        return {
          description,
          parameters: Parameters,
          execute: (params: z.infer<typeof Parameters>, ctx: Tool.Context) =>
            Effect.gen(function* () {
              const cwd = yield* fs.getWorkingDirectory()
              const scriptPath = params.scriptPath ?? "./check.mjs"

              // Ensure the scriptPath is absolute or relative to cwd
              const absolutePath = path.isAbsolute(scriptPath)
                ? scriptPath
                : path.join(cwd, scriptPath)

              // Check if the file exists
              const exists = yield* fs.fileExists(absolutePath)
              if (!exists) {
                return {
                  status: "ERROR" as const,
                  passed: 0,
                  failed: 0,
                  summary: `Verification script not found: ${scriptPath}`,
                }
              }

              return yield* run(
                {
                  scriptPath: absolutePath,
                  cwd,
                },
                ctx
              )
            }),
        }
      })
  }),
)