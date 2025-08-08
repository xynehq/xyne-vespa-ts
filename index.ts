// Main exports for the Vespa package
export { VespaService } from "./src/vespa";
export { createVespaService, NoOpLogger, ConsoleLogger, createDefaultConfig } from "./src/factory";
export type { 
  ILogger, 
  VespaConfig, 
  VespaDependencies, 
} from "./src/interfaces";

export * from "./src/client";
export * from "./src/errors";
export * from "./src/types";
export * from "./src/mappers";
