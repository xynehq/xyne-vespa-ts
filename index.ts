// Main exports for the Vespa package
export { VespaService } from "./src/vespa"
export { createVespaService, ConsoleLogger, createDefaultConfig } from "./src"
export type {
  ILogger,
  VespaConfig,
  VespaDependencies,
} from "./src/types"

export * from "./src/client"
export * from "./src/errors"
export * from "./src/types"
export * from "./src/mappers"
