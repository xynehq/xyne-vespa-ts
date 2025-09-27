import {
  And,
  Or,
  Exclude,
  Include,
  NearestNeighbor,
  Not,
  Parenthesized,
  Raw,
  Timestamp,
  UserInput,
  VespaField,
} from "./conditions"
import { FieldName, FieldValue, TimestampRange, YqlCondition } from "./types"

const and = (conditions: YqlCondition[], requirePermissions = false): And =>
  new And(conditions, requirePermissions)
const or = (conditions: YqlCondition[], requirePermissions = false): Or =>
  new Or(conditions, requirePermissions)

const andWithoutPermissions = (conditions: YqlCondition[]): And =>
  new And(conditions, false)
const orWithoutPermissions = (conditions: YqlCondition[]): Or =>
  new Or(conditions, false)
const not = (condition: YqlCondition): Not => new Not(condition)
const parenthesize = (condition: YqlCondition): Parenthesized =>
  new Parenthesized(condition)
const timestamp = (
  fromField: FieldName,
  toField: FieldName,
  range: TimestampRange,
): Timestamp => new Timestamp(fromField, toField, range)
const exclude = (docIds: string[]): Exclude => new Exclude(docIds)
const include = (field: FieldName, values: string[]): Include =>
  new Include(field, values)
const raw = (condition: string): Raw => new Raw(condition)
const userInput = (
  queryParam: string = "@query",
  targetHits: number,
): UserInput => new UserInput(queryParam, targetHits)
const nearestNeighbor = (
  field: FieldName,
  queryParam: string = "e",
  targetHits: number,
): NearestNeighbor => new NearestNeighbor(field, queryParam, targetHits)

const contains = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.contains(field, value)
const matches = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.matches(field, value)
const equals = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.equals(field, value)
const greaterThan = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.greaterThan(field, value)
const greaterThanOrEqual = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.greaterThanOrEqual(field, value)
const lessThan = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.lessThan(field, value)
const lessThanOrEqual = (field: FieldName, value: FieldValue): VespaField =>
  VespaField.lessThanOrEqual(field, value)

export {
  and,
  or,
  andWithoutPermissions,
  orWithoutPermissions,
  not,
  parenthesize,
  timestamp,
  exclude,
  include,
  raw,
  userInput,
  nearestNeighbor,
  contains,
  matches,
  equals,
  greaterThan,
  greaterThanOrEqual,
  lessThan,
  lessThanOrEqual,
}
