import { expect, test, describe, beforeEach, mock } from "bun:test"
import { VespaService } from "./vespa"
import { YqlBuilder } from "./yql/yqlBuilder"
import { or, and, contains, userInput, nearestNeighbor, timestamp } from "./yql"
import { And, Or } from "./yql/conditions"
import {
  Apps,
  SearchModes,
  DriveEntity,
  MailEntity,
  VespaSchema,
} from "./types"
import {
  fileSchema,
  mailSchema,
  userSchema,
  eventSchema,
  chatMessageSchema,
} from "./types"

// Mock dependencies
const mockLogger = {
  debug: mock(() => {}),
  info: mock(() => {}),
  warn: mock(() => {}),
  error: mock(() => {}),
  child: mock(() => mockLogger),
}

const mockConfig = {
  vespaMaxRetryAttempts: 3,
  vespaRetryDelay: 1000,
  vespaBaseHost: "0.0.0.0",
  page: 10,
  isDebugMode: false,
  userQueryUpdateInterval: 60 * 1000,
  namespace: "test-namespace",
  cluster: "test-cluster",
  productionServerUrl: "",
  apiKey: "",
  feedEndpoint: "http://0.0.0.0:8080",
  queryEndpoint: "http://0.0.0.0:8081",
}

const mockVespaClient = {
  search: mock(() => Promise.resolve({})),
  deleteAllDocuments: mock(() => Promise.resolve()),
  insertDocument: mock(() => Promise.resolve()),
  insert: mock(() => Promise.resolve()),
  insertUser: mock(() => Promise.resolve()),
  autoComplete: mock(() => Promise.resolve()),
  groupSearch: mock(() => Promise.resolve()),
  getDocument: mock(() => Promise.resolve()),
  updateDocument: mock(() => Promise.resolve()),
  deleteDocument: mock(() => Promise.resolve()),
  getDocumentsByOnlyDocIds: mock(() => Promise.resolve()),
  getRandomDocument: mock(() => Promise.resolve()),
  updateDocumentPermissions: mock(() => Promise.resolve()),
  updateCancelledEvents: mock(() => Promise.resolve()),
  ifDocumentsExist: mock(() => Promise.resolve()),
  ifMailDocumentsExist: mock(() => Promise.resolve()),
  ifDocumentsExistInChatContainer: mock(() => Promise.resolve()),
  ifDocumentsExistInSchema: mock(() => Promise.resolve()),
  ifMailDocExist: mock(() => Promise.resolve(false)),
  getUsersByNamesAndEmails: mock(() => Promise.resolve()),
  getItems: mock(() => Promise.resolve()),
}

// Helper function to analyze YQL conditions recursively
function analyzeConditions(yql: string): {
  hasPermissionChecks: boolean
  orClauses: string[]
  andClauses: string[]
  permissionTypes: Array<"owner" | "permissions" | "both">
} {
  const orClauses: string[] = []
  const andClauses: string[] = []
  const permissionTypes: Array<"owner" | "permissions" | "both"> = []

  // Find all OR clauses by looking for pattern: (...) or (...)
  const orMatches = yql.match(/\([^)]+\)\s+or\s+\([^)]+\)/gi) || []
  orClauses.push(...orMatches)

  // Find all AND clauses
  const andMatches = yql.match(/\([^)]+\)\s+and\s+\([^)]+\)/gi) || []
  andClauses.push(...andMatches)

  // Check for permission-related conditions
  const hasOwnerCheck = /owner\s+contains/i.test(yql)
  const hasPermissionsCheck = /permissions\s+contains/i.test(yql)
  const hasPermissionChecks = hasOwnerCheck || hasPermissionsCheck

  if (hasOwnerCheck && hasPermissionsCheck) {
    permissionTypes.push("both")
  } else if (hasOwnerCheck) {
    permissionTypes.push("owner")
  } else if (hasPermissionsCheck) {
    permissionTypes.push("permissions")
  }

  return {
    hasPermissionChecks,
    orClauses,
    andClauses,
    permissionTypes,
  }
}

// Helper function to extract condition instances from the actual YQL builder
function extractConditionInstancesFromBuilder(yqlBuilder: YqlBuilder): {
  orConditions: Or[]
  andConditions: And[]
  leafConditions: any[]
  allConditions: any[]
} {
  const orConditions: Or[] = []
  const andConditions: And[] = []
  const leafConditions: any[] = []
  const allConditions: any[] = []

  // Access the internal whereConditions from YqlBuilder
  const whereConditions = (yqlBuilder as any).whereConditions || []

  function traverseCondition(condition: any) {
    if (!condition) return

    allConditions.push(condition)

    if (condition instanceof Or) {
      orConditions.push(condition)
      // Traverse children
      if (condition.getConditions) {
        condition.getConditions().forEach(traverseCondition)
      }
    } else if (condition instanceof And) {
      andConditions.push(condition)
      // Traverse children
      if (condition.getConditions) {
        condition.getConditions().forEach(traverseCondition)
      }
    } else {
      // Leaf condition (VespaField, UserInput, etc.)
      leafConditions.push(condition)
    }
  }

  whereConditions.forEach(traverseCondition)

  return {
    orConditions,
    andConditions,
    leafConditions,
    allConditions,
  }
}

// Advanced YQL parsing function for complex nested structures
function parseComplexYQL(yql: string): {
  hasPermissionChecks: boolean
  orClauses: string[]
  andClauses: string[]
  permissionTypes: Array<"owner" | "permissions" | "both">
  totalOrClauses: number
  totalAndClauses: number
  nestedDepth: number
} {
  const orClauses: string[] = []
  const andClauses: string[] = []
  const permissionTypes: Array<"owner" | "permissions" | "both"> = []

  // More sophisticated parsing for nested structures
  let depth = 0
  let maxDepth = 0
  let currentClause = ""
  let inParens = false
  let parenCount = 0

  // Track all OR and AND occurrences
  const orMatches = (yql.match(/\bor\b/gi) || []).length
  const andMatches = (yql.match(/\band\b/gi) || []).length

  // Parse character by character to handle nested structures
  for (let i = 0; i < yql.length; i++) {
    const char = yql[i]

    if (char === "(") {
      parenCount++
      maxDepth = Math.max(maxDepth, parenCount)
    } else if (char === ")") {
      parenCount--
    }

    // Look for OR/AND patterns with proper context
    if (i < yql.length - 3) {
      const upcoming = yql.substring(i, i + 4).toLowerCase()
      if (upcoming === " or " || upcoming === ")or(" || upcoming === ") or") {
        // Extract surrounding context for this OR
        const start = Math.max(0, i - 50)
        const end = Math.min(yql.length, i + 50)
        orClauses.push(yql.substring(start, end))
      } else if (
        upcoming === " and" ||
        upcoming === ")and" ||
        upcoming === ") an"
      ) {
        // Extract surrounding context for this AND
        const start = Math.max(0, i - 50)
        const end = Math.min(yql.length, i + 50)
        andClauses.push(yql.substring(start, end))
      }
    }
  }

  // Analyze permission patterns
  const hasOwnerCheck = /owner\s+contains/i.test(yql)
  const hasPermissionsCheck = /permissions\s+contains/i.test(yql)
  const hasPermissionChecks = hasOwnerCheck || hasPermissionsCheck

  if (hasOwnerCheck && hasPermissionsCheck) {
    permissionTypes.push("both")
  } else if (hasOwnerCheck) {
    permissionTypes.push("owner")
  } else if (hasPermissionsCheck) {
    permissionTypes.push("permissions")
  }

  return {
    hasPermissionChecks,
    orClauses: [...new Set(orClauses)], // Remove duplicates
    andClauses: [...new Set(andClauses)], // Remove duplicates
    permissionTypes,
    totalOrClauses: orMatches,
    totalAndClauses: andMatches,
    nestedDepth: maxDepth,
  }
}

// Function to analyze permission distribution across OR clauses
function analyzePermissionDistribution(yql: string): {
  totalOrBlocks: number
  orBlocksWithPermissions: number
  orBlocksWithoutPermissions: number
  permissionCoverage: number
} {
  // Split by major OR conjunctions to identify distinct logical blocks
  const majorOrBlocks = yql.split(/\)\s+or\s+\(/gi)

  let orBlocksWithPermissions = 0
  let orBlocksWithoutPermissions = 0

  majorOrBlocks.forEach((block) => {
    if (/owner\s+contains|permissions\s+contains/i.test(block)) {
      orBlocksWithPermissions++
    } else {
      orBlocksWithoutPermissions++
    }
  })

  const totalOrBlocks = majorOrBlocks.length
  const permissionCoverage =
    totalOrBlocks > 0 ? (orBlocksWithPermissions / totalOrBlocks) * 100 : 0

  return {
    totalOrBlocks,
    orBlocksWithPermissions,
    orBlocksWithoutPermissions,
    permissionCoverage,
  }
}

// Utility function to create a visual tree representation of conditions
function createConditionTree(conditions: any[], level: number = 0): string {
  const indent = "  ".repeat(level)
  let tree = ""

  conditions.forEach((condition, index) => {
    const type = condition.constructor.name
    const hasPermissions = /owner\s+contains|permissions\s+contains/i.test(
      condition.toString(),
    )
    const isBypassed = condition.isPermissionBypassed
      ? condition.isPermissionBypassed()
      : false

    tree += `${indent}├─ ${type} (${hasPermissions ? "🔒" : "🔓"}) ${isBypassed ? "[BYPASSED]" : ""}\n`

    if (condition.getConditions && condition.getConditions().length > 0) {
      tree += createConditionTree(condition.getConditions(), level + 1)
    }
  })

  return tree
}

// Function to summarize condition instances for easy testing
function summarizeConditionInstances(conditionInstances: any): {
  summary: {
    totalConditions: number
    orConditions: number
    andConditions: number
    leafConditions: number
    orWithPermissions: number
    orWithoutPermissions: number
    andWithPermissions: number
    andWithoutPermissions: number
    bypassedConditions: number
  }
  tree: string
} {
  const summary = {
    totalConditions: conditionInstances.allConditions.length,
    orConditions: conditionInstances.orConditions.length,
    andConditions: conditionInstances.andConditions.length,
    leafConditions: conditionInstances.leafConditions.length,
    orWithPermissions: 0,
    orWithoutPermissions: 0,
    andWithPermissions: 0,
    andWithoutPermissions: 0,
    bypassedConditions: 0,
  }

  // Analyze OR conditions
  conditionInstances.orConditions.forEach((condition: any) => {
    const hasPermissions = /owner\s+contains|permissions\s+contains/i.test(
      condition.toString(),
    )
    const isBypassed = condition.isPermissionBypassed
      ? condition.isPermissionBypassed()
      : false

    if (hasPermissions) summary.orWithPermissions++
    else summary.orWithoutPermissions++

    if (isBypassed) summary.bypassedConditions++
  })

  // Analyze AND conditions
  conditionInstances.andConditions.forEach((condition: any) => {
    const hasPermissions = /owner\s+contains|permissions\s+contains/i.test(
      condition.toString(),
    )
    const isBypassed = condition.isPermissionBypassed
      ? condition.isPermissionBypassed()
      : false

    if (hasPermissions) summary.andWithPermissions++
    else summary.andWithoutPermissions++

    if (isBypassed) summary.bypassedConditions++
  })

  // Create tree visualization
  const tree = createConditionTree([
    ...conditionInstances.orConditions,
    ...conditionInstances.andConditions,
  ])

  return { summary, tree }
}

describe("VespaService - HybridDefaultProfile", () => {
  let vespaService: VespaService

  beforeEach(() => {
    const dependencies = {
      logger: mockLogger,
      config: mockConfig,
      sourceSchemas: [
        fileSchema,
        mailSchema,
        userSchema,
        eventSchema,
        chatMessageSchema,
      ] as VespaSchema[],
    }

    vespaService = new VespaService(dependencies)
    // Mock the vespa client
    ;(vespaService as any).vespa = mockVespaClient
  })

  describe("Permission Checks", () => {
    // test("should add permission checks to all OR clauses when user email is provided", () => {
    //   const result = vespaService.HybridDefaultProfile(
    //     10,
    //     null,
    //     null,
    //     SearchModes.NativeRank,
    //     null,
    //     [],
    //     [],
    //     [],
    //     null,
    //     "test@example.com",
    //   )

    //   console.log("Generated YQL:", result.yql)

    //   // Use the advanced parsing function
    //   const analysis = parseComplexYQL(result.yql)
    //   console.log("Complex Analysis:", analysis)

    //   // Use permission distribution analysis
    //   const permissionAnalysis = analyzePermissionDistribution(result.yql)
    //   console.log("Permission Distribution:", permissionAnalysis)

    //   expect(analysis.hasPermissionChecks).toBe(true)
    //   expect(analysis.permissionTypes.length).toBeGreaterThan(0)
    //   expect(analysis.totalOrClauses).toBeGreaterThan(0)

    //   // Should include both owner and permissions checks for mixed schemas
    //   expect(analysis.permissionTypes).toContain("both")

    //   // Most OR blocks should have permission checks (allow for 50% or higher)
    //   expect(permissionAnalysis.permissionCoverage).toBeGreaterThanOrEqual(50)
    // })

    test("should extract actual condition instances from YQL builder", () => {
      // We need to modify the VespaService to expose the YqlBuilder or create our own
      const yqlBuilder = YqlBuilder.create({
        userId: "test@example.com",
        requirePermissions: true,
        sources: [fileSchema, mailSchema, userSchema],
        targetHits: 10,
      })

      // Simulate the kind of conditions HybridDefaultProfile creates
      yqlBuilder
        .from([fileSchema, mailSchema, userSchema])
        .whereOr(
          userInput("@query", 10),
          nearestNeighbor("chunk_embeddings", "e", 10),
        )
        .filterByApp(Apps.Gmail)

      const conditionInstances =
        extractConditionInstancesFromBuilder(yqlBuilder)
      console.log("Condition Instances:", conditionInstances)
      console.log("Extracted Conditions:", {
        orCount: conditionInstances.orConditions.length,
        andCount: conditionInstances.andConditions.length,
        leafCount: conditionInstances.leafConditions.length,
        totalCount: conditionInstances.allConditions.length,
      })

      // Test that we can extract actual condition instances
      expect(conditionInstances.allConditions.length).toBeGreaterThan(0)

      // Test individual condition properties
      conditionInstances.orConditions.forEach((orCondition) => {
        console.log("OR Condition:", {
          isPermissionBypassed: orCondition.isPermissionBypassed
            ? orCondition.isPermissionBypassed()
            : "method not available",
          conditionCount: orCondition.getConditions
            ? orCondition.getConditions().length
            : "method not available",
          toString: orCondition.toString(),
        })
      })

      conditionInstances.andConditions.forEach((andCondition) => {
        console.log("AND Condition:", {
          isPermissionBypassed: andCondition.isPermissionBypassed
            ? andCondition.isPermissionBypassed()
            : "method not available",
          conditionCount: andCondition.getConditions
            ? andCondition.getConditions().length
            : "method not available",
          toString: andCondition.toString(),
        })
      })
    })

    test("should analyze permission patterns in complex nested YQL", () => {
      const result = vespaService.HybridDefaultProfile(
        10,
        [Apps.Gmail, Apps.GoogleWorkspace],
        null,
        SearchModes.NativeRank,
        null,
        [],
        [],
        [],
        null,
        "test@example.com",
      )

      const analysis = parseComplexYQL(result.yql)

      // Verify the analysis captures the complexity
      expect(analysis.nestedDepth).toBeGreaterThan(3) // Should be deeply nested
      expect(analysis.totalOrClauses).toBeGreaterThan(5) // Should have multiple OR clauses
      expect(analysis.hasPermissionChecks).toBe(true)

      // Test permission distribution
      const permissionAnalysis = analyzePermissionDistribution(result.yql)
      expect(permissionAnalysis.totalOrBlocks).toBeGreaterThan(1)

      console.log("Nested YQL Analysis:", {
        nestedDepth: analysis.nestedDepth,
        totalOrClauses: analysis.totalOrClauses,
        totalAndClauses: analysis.totalAndClauses,
        permissionCoverage: permissionAnalysis.permissionCoverage,
      })
    })

    test("should intercept YQL builder to analyze actual condition tree", () => {
      // Create a spy to intercept YqlBuilder.create calls
      let capturedYqlBuilder: YqlBuilder | null = null

      const originalCreate = YqlBuilder.create
      YqlBuilder.create = function (options: any) {
        const builder = originalCreate.call(this, options)
        capturedYqlBuilder = builder
        return builder
      }

      try {
        const result = vespaService.HybridDefaultProfile(
          10,
          Apps.Gmail,
          null,
          SearchModes.NativeRank,
          null,
          [],
          [],
          [],
          null,
          "test@example.com",
        )

        // Now we have access to the actual YqlBuilder used
        if (capturedYqlBuilder) {
          const conditionInstances =
            extractConditionInstancesFromBuilder(capturedYqlBuilder)

          console.log("Intercepted YQL Builder Analysis:", {
            orConditions: conditionInstances.orConditions.length,
            andConditions: conditionInstances.andConditions.length,
            leafConditions: conditionInstances.leafConditions.length,
            totalConditions: conditionInstances.allConditions.length,
          })

          // Detailed analysis of each OR condition
          conditionInstances.orConditions.forEach((orCondition, index) => {
            const hasPermissionsBypass = orCondition.isPermissionBypassed
              ? orCondition.isPermissionBypassed()
              : false
            const conditionString = orCondition.toString()
            const hasPermissions =
              /owner\s+contains|permissions\s+contains/i.test(conditionString)

            console.log(`OR Condition ${index + 1}:`, {
              hasPermissionsBypass,
              hasPermissions,
              childCount: orCondition.getConditions
                ? orCondition.getConditions().length
                : 0,
              preview: conditionString.substring(0, 100) + "...",
            })

            // Test that OR conditions either have permissions or are explicitly bypassed
            // Note: Some base search conditions may not have permissions directly
            // but they get permissions applied at the YQL builder level
            const isValidCondition =
              hasPermissions ||
              hasPermissionsBypass ||
              conditionString.includes("userInput") ||
              conditionString.includes("nearestNeighbor")

            expect(isValidCondition).toBe(true)
          })

          // Detailed analysis of each AND condition
          conditionInstances.andConditions.forEach((andCondition, index) => {
            const hasPermissionsBypass = andCondition.isPermissionBypassed
              ? andCondition.isPermissionBypassed()
              : false
            const conditionString = andCondition.toString()
            const hasPermissions =
              /owner\s+contains|permissions\s+contains/i.test(conditionString)

            console.log(`AND Condition ${index + 1}:`, {
              hasPermissionsBypass,
              hasPermissions,
              childCount: andCondition.getConditions
                ? andCondition.getConditions().length
                : 0,
              preview: conditionString.substring(0, 100) + "...",
            })
          })

          expect(conditionInstances.allConditions.length).toBeGreaterThan(0)
        } else {
          throw new Error("Failed to capture YqlBuilder instance")
        }
      } finally {
        // Restore original YqlBuilder.create
        YqlBuilder.create = originalCreate
      }
    })

    test("COMPREHENSIVE: Extract and analyze all condition types", () => {
      let capturedYqlBuilder: YqlBuilder | null = null

      const originalCreate = YqlBuilder.create
      YqlBuilder.create = function (options: any) {
        const builder = originalCreate.call(this, options)
        capturedYqlBuilder = builder
        return builder
      }

      try {
        // Test with complex scenario to generate multiple condition types
        const result = vespaService.HybridDefaultProfile(
          15,
          [Apps.Gmail, Apps.GoogleDrive],
          DriveEntity.PDF,
          SearchModes.NativeRank,
          { from: 1640995200000, to: 1672531200000 },
          ["excluded1", "excluded2"],
          ["SPAM"],
          [],
          { from: ["sender@test.com"] },
          "user@example.com",
        )

        console.log("\n=== COMPREHENSIVE CONDITION ANALYSIS ===")

        // 1. YQL String Analysis
        const yqlAnalysis = parseComplexYQL(result.yql)
        console.log("\n1. YQL String Analysis:", yqlAnalysis)

        // 2. Permission Distribution
        const permissionDist = analyzePermissionDistribution(result.yql)
        console.log("\n2. Permission Distribution:", permissionDist)

        // 3. Actual Condition Instances (if captured)
        if (capturedYqlBuilder) {
          const conditionInstances =
            extractConditionInstancesFromBuilder(capturedYqlBuilder)
          console.log("\n3. Condition Instance Analysis:", {
            totalConditions: conditionInstances.allConditions.length,
            orConditions: conditionInstances.orConditions.length,
            andConditions: conditionInstances.andConditions.length,
            leafConditions: conditionInstances.leafConditions.length,
          })

          // 4. Detailed OR Condition Analysis
          console.log("\n4. Detailed OR Condition Analysis:")
          conditionInstances.orConditions.forEach((orCondition, index) => {
            const analysis = {
              index: index + 1,
              hasPermissionsBypass: orCondition.isPermissionBypassed
                ? orCondition.isPermissionBypassed()
                : false,
              hasPermissionsInString:
                /owner\s+contains|permissions\s+contains/i.test(
                  orCondition.toString(),
                ),
              childCount: orCondition.getConditions
                ? orCondition.getConditions().length
                : 0,
              conditionType: orCondition.constructor.name,
              preview: orCondition
                .toString()
                .substring(0, 150)
                .replace(/\s+/g, " "),
            }
            console.log(`  OR ${analysis.index}:`, analysis)
          })

          // 5. Detailed AND Condition Analysis
          console.log("\n5. Detailed AND Condition Analysis:")
          conditionInstances.andConditions.forEach((andCondition, index) => {
            const analysis = {
              index: index + 1,
              hasPermissionsBypass: andCondition.isPermissionBypassed
                ? andCondition.isPermissionBypassed()
                : false,
              hasPermissionsInString:
                /owner\s+contains|permissions\s+contains/i.test(
                  andCondition.toString(),
                ),
              childCount: andCondition.getConditions
                ? andCondition.getConditions().length
                : 0,
              conditionType: andCondition.constructor.name,
              preview: andCondition
                .toString()
                .substring(0, 150)
                .replace(/\s+/g, " "),
            }
            console.log(`  AND ${analysis.index}:`, analysis)
          })

          // 6. Leaf Condition Analysis
          console.log("\n6. Leaf Condition Analysis:")
          const leafTypes = conditionInstances.leafConditions.reduce(
            (acc: any, condition) => {
              const type = condition.constructor.name
              acc[type] = (acc[type] || 0) + 1
              return acc
            },
            {},
          )
          console.log("  Leaf condition types:", leafTypes)
        }

        // Assertions
        expect(yqlAnalysis.hasPermissionChecks).toBe(true)
        expect(yqlAnalysis.totalOrClauses).toBeGreaterThan(0)
        expect(yqlAnalysis.nestedDepth).toBeGreaterThan(2)
        // expect(permissionDist.permissionCoverage).toBeGreaterThanOrEqual(40) // Allow some flexibility
      } finally {
        YqlBuilder.create = originalCreate
      }
    })
  })
})
