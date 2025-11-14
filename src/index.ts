import type { VespaDependencies, ILogger, VespaConfig } from "./types"
import { VespaService } from "./vespa"

/**
 * Factory function to create a configured VespaService instance
 */
export function createVespaService(
  dependencies: VespaDependencies,
): VespaService {
  return new VespaService(dependencies)
}

export class ConsoleLogger implements ILogger {
  private metadata: Record<string, any> = {}

  constructor(metadata: Record<string, any> = {}) {
    this.metadata = metadata
  }

  debug(message: string, ...args: any[]): void {
    console.debug(`[DEBUG]${this.formatMetadata()} ${message}`, ...args)
  }

  info(message: string, ...args: any[]): void {
    console.info(`[INFO]${this.formatMetadata()} ${message}`, ...args)
  }

  warn(message: string, ...args: any[]): void {
    console.warn(`[WARN]${this.formatMetadata()} ${message}`, ...args)
  }

  error(message: string | Error, ...args: any[]): void {
    const msg = message instanceof Error ? message.message : message
    console.error(`[ERROR]${this.formatMetadata()} ${msg}`, ...args)
  }

  child(metadata: Record<string, any>): ILogger {
    return new ConsoleLogger({ ...this.metadata, ...metadata })
  }

  private formatMetadata(): string {
    const keys = Object.keys(this.metadata)
    if (keys.length === 0) return ""

    const formatted = keys
      .map((key) => `${key}:${this.metadata[key]}`)
      .join(", ")
    return ` [${formatted}]`
  }
}

/**
 * Helper function to create default config
 */
export function createDefaultConfig(
  overrides: Partial<VespaConfig> = {},
): VespaConfig {
  const defaultConfig = {
    vespaMaxRetryAttempts: 3,
    vespaRetryDelay: 1000,
    vespaBaseHost: "localhost",
    page: 10,
    isDebugMode: false,
    userQueryUpdateInterval: 60 * 1000,
    namespace: "namespace",
    cluster: "my_content",
    productionServerUrl: "",
    apiKey: "",
    vespaEndpoint: {
      feedEndpoint: "http://0.0.0.0:8080",
      queryEndpoint: "http://0.0.0.0:8081",
    },
  }

  return {
    ...defaultConfig,
    ...overrides,
  }
}

export { VespaService } from "./vespa"
export type { VespaDependencies, ILogger, VespaConfig } from "./types"
