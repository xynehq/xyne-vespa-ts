import { YqlCondition, PermissionFilter, PermissionFieldType } from "./types"
import { BaseCondition, VespaField, Or, And } from "./conditions"
import { and } from "."

export class PermissionCondition extends BaseCondition {
  constructor(
    private userEmail: string,
    private includeOwnerCheck: boolean = true,
    private includePermissionCheck: boolean = true,
  ) {
    super()
    if (!userEmail || !userEmail.trim()) {
      throw new Error("User email is required for permission checks")
    }
  }

  toString(): string {
    const conditions: string[] = []

    if (this.includePermissionCheck) {
      conditions.push("permissions contains @email")
    }

    if (this.includeOwnerCheck) {
      conditions.push("owner contains @email")
    }

    if (conditions.length === 0) {
      throw new Error("At least one permission check must be enabled")
    }

    return conditions.length === 1
      ? conditions[0]!
      : `(${conditions.join(" or ")})`
  }

  /**
   * Creates a permission condition that only checks permissions field
   */
  static EmailPermissions(userEmail: string): PermissionCondition {
    return new PermissionCondition(userEmail, false, true)
  }

  /**
   * Creates a permission condition that only checks owner field
   */
  static EmailOwner(userEmail: string): PermissionCondition {
    return new PermissionCondition(userEmail, true, false)
  }
}

/**
 * Wrapper that ensures any condition is combined with proper permissions
 */
export class PermissionWrapper {
  constructor(private userEmail: string) {
    if (!userEmail || !userEmail.trim()) {
      throw new Error("User email is required for permission wrapper")
    }
  }
  wrapEmailPermission(
    condition: YqlCondition,
    requirePermissions: boolean = true,
  ): YqlCondition {
    if (!requirePermissions) {
      return condition
    }

    const permissionCondition = PermissionCondition.EmailPermissions(
      this.userEmail,
    )
    return and([condition, permissionCondition])
  }

  wrapEmailOwner(
    condition: YqlCondition,
    requirePermissions: boolean = true,
  ): YqlCondition {
    if (!requirePermissions) {
      return condition
    }

    const permissionCondition = PermissionCondition.EmailOwner(this.userEmail)
    return and([condition, permissionCondition])
  }
}
