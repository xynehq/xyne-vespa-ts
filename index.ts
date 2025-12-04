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
export * from "./src/yql"
export { YqlBuilder } from "./src/yql/yqlBuilder"
export type {
  YqlCondition,
  YqlBuilderOptions,
  TimestampRange,
  FieldName,
  FieldValue,
  YqlProfile,
  PermissionOptions,
} from "./src/yql/types"

export { Operator, PermissionFieldType } from "./src/yql/types"
