import { expect, test, describe } from "bun:test"
import {
  or,
  and,
  contains,
  userInput,
  nearestNeighbor,
  not,
  andWithPermissions,
  andWithoutPermissions,
} from "../../index"
import {
  Apps,
  DriveEntity,
  fileSchema,
  MailEntity,
  mailSchema,
  userSchema,
} from "../types"
import { formatYqlToReadable } from "../utils"
import { YqlBuilder } from "../yql/yqlBuilder"

describe("YqlBuilder - Permission Application", () => {
  describe("Global Level Permissions (no without Permissions calls)", () => {
    test("should apply permissions at global level for simple OR condition", () => {
      const builder = YqlBuilder.create({
        userId: "test@example.com",
        requirePermissions: true,
        sources: [fileSchema, mailSchema],
      })

      const yql = builder
        .from([fileSchema, mailSchema])
        .where(
          and([
            or([
              contains("app", Apps.Gmail),
              contains("app", Apps.GoogleDrive),
            ]),
            or([
              contains("entity", MailEntity.Email),
              contains("entity", DriveEntity.Docs),
            ]),
          ]),
        )
        .build()
      // Count how many times permissions appear in the query
      const permissionMatches = (yql.match(/permissions contains/gi) || [])
        .length
      console.log("Permission occurrences:", permissionMatches)

      // Should have permissions applied only at global level (once)
      expect(permissionMatches).toBe(1)

      // Should have the pattern: (condition) and permissions contains
      expect(yql).toMatch(/\)\s+and\s+permissions contains/)
    })

    test("should apply permissions at global level for nested OR/AND conditions", () => {
      const builder = YqlBuilder.create({
        userId: "test@example.com",
        requirePermissions: true,
        sources: [fileSchema, mailSchema],
      })

      const yql = builder
        .from([fileSchema, mailSchema])
        .whereOr(
          and([contains("app", Apps.Gmail), contains("entity", "email")]),
          and([contains("app", Apps.GoogleDrive), contains("entity", "file")]),
        )
        .build()

      console.log("Nested OR/AND YQL:", formatYqlToReadable(yql))

      // Count permission occurrences
      const permissionMatches = (yql.match(/permissions contains/gi) || [])
        .length
      console.log("Permission occurrences:", permissionMatches)

      // Should have permissions applied only at global level (once)
      expect(permissionMatches).toBe(1)

      // Should have the pattern at the end: ) and permissions contains
      expect(yql).toMatch(/\)\s+and\s+permissions contains/)
    })

    test("should apply permission to each level if some clause permission is by passed", () => {
      const builder = YqlBuilder.create({
        userId: "test@example.com",
        requirePermissions: true,
        sources: [fileSchema],
      })

      const conditions = andWithoutPermissions([
        or([
          userInput("@query", 10),
          nearestNeighbor("chunk_embeddings", "e", 10),
        ]),
        and([contains("app", Apps.Gmail), contains("entity", "email")]),
        andWithoutPermissions([
          contains("app", Apps.GoogleDrive),
          contains("entity", "file"),
        ]),
      ])
      const yql = builder
        .from([fileSchema])
        .where(conditions)
        .filterByApp(Apps.Gmail)
        .build()

      console.log(formatYqlToReadable(yql))

      // Count permission occurrences
      const permissionMatches = (yql.match(/permissions contains/gi) || [])
        .length
      console.log("Permission occurrences:", permissionMatches)

      // Should have permissions applied only at global level (once)
      expect(permissionMatches).toBe(3)
    })

    test("should apply permission to global level only", () => {
      const builder = YqlBuilder.create({
        userId: "test@example.com",
        requirePermissions: true,
        sources: [fileSchema],
      })

      const conditions = and([
        or([
          userInput("@query", 10),
          nearestNeighbor("chunk_embeddings", "e", 10),
        ]),
        and([contains("app", Apps.Gmail), contains("entity", "email")]),
        and([contains("app", Apps.GoogleDrive), contains("entity", "file")]),
      ])
      const yql = builder
        .from([fileSchema])
        .where(conditions)
        .filterByApp(Apps.Gmail)
        .build()

      // Count permission occurrences
      const permissionMatches = (yql.match(/permissions contains/gi) || [])
        .length
      console.log("Permission occurrences:", permissionMatches)

      // Should have permissions applied only at global level (once)
      expect(permissionMatches).toBe(1)
    })
  })
})
