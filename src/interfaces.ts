import type { Apps, Entity, VespaSchema } from "./types";
import config from "./config";
/**
 * Logger interface for dependency injection
 * Compatible with the original logger interface from xyne
 */
export interface ILogger {
  debug(message: string, ...args: any[]): void;
  info(message: string, ...args: any[]): void;
  warn(message: string, ...args: any[]): void;
  error(message: string | Error, ...args: any[]): void;
  child(metadata: Record<string, any>): ILogger;
}

 /**
 * Vespa configuration interface
 */
export type VespaConfig = typeof config;

/**
 * Main dependencies interface for dependency injection
 */
export interface VespaDependencies {
  logger: ILogger;
  config: VespaConfig;
  sourceSchemas: string[];
  vespaEndpoint: string;
}
