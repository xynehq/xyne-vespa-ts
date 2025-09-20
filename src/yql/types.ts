import { SearchModes } from "../types"

export interface YqlCondition {
  toString(): string
}

export interface YqlClause {
  toString(): string
}

export interface TimestampRange {
  from?: number | null
  to?: number | null
}

export interface PermissionFilter {
  email?: string
  permissions?: string[]
  owner?: string
}

export interface YqlProfile {
  profile: SearchModes
  yql: string
}

export interface YqlBuilderOptions {
  sources?: string[]
  targetHits?: number
  limit?: number
  offset?: number
  timeout?: string
  requirePermissions?: boolean
  validateSyntax?: boolean
}

export type FieldName = string
export type FieldValue = string | number | boolean
export type LogicalOperator = "and" | "or"
export type ComparisonOperator =
  | "contains"
  | ">="
  | "<="
  | "="
  | "=~"
  | ">"
  | "<"

export interface SearchCondition {
  field: FieldName
  operator: ComparisonOperator
  value: FieldValue
}

export interface NearestNeighborCondition {
  field: FieldName
  queryParam: string
  targetHits?: number
}

export interface UserInputCondition {
  queryParam: string
  targetHits?: number
}

export enum PermissionFieldType {
  OWNER = "owner",
  PERMISSIONS = "permissions",
  BOTH = "both",
}
