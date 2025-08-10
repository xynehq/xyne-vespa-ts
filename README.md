# @xyne/vespa-ts

A reusable TypeScript package for interacting with Vespa search engine with dependency injection support.

## Installation

```bash
npm install @xyne/vespa-ts
```

## Modular Imports

This package supports modular imports, allowing you to import only the parts you need:

### Core Functions (Default Import)

```typescript
// Import core functionality
import { createVespaService, ConsoleLogger, VespaService } from '@xyne/vespa-ts';
import type { VespaDependencies, ILogger, VespaConfig } from '@xyne/vespa-ts';
```

### Types

```typescript
// Import types separately
import type { 
  VespaSearchResponse, 
  VespaFileSearch, 
  VespaUser,
  SearchResponse,
  AutocompleteResults,
  Apps,
  Entity
} from '@xyne/vespa-ts/types';
```

### Mappers

```typescript
// Import transformation functions
import { 
  VespaSearchResponseToSearchResult,
  VespaAutocompleteResponseToResult,
  getSortedScoredChunks,
  handleVespaGroupResponse
} from '@xyne/vespa-ts/mappers';
```

### Utils

```typescript
// Import utility functions
import { 
  scale, 
  getErrorMessage, 
  escapeYqlValue,
  processGmailIntent,
  dateToUnixTimestamp
} from '@xyne/vespa-ts/utils';
```

### Errors

```typescript
// Import custom error classes
import { 
  ErrorPerformingSearch,
  ErrorRetrievingDocuments,
  ErrorInsertingDocument,
  ErrorDeletingDocuments
} from '@xyne/vespa-ts/errors';
```

### Client

```typescript
// Import specific client implementations
import { ProductionVespaClient } from '@xyne/vespa-ts/client';
import vespaClient from '@xyne/vespa-ts/client'; // default client
```

## Usage Example

```typescript
import { createVespaService, ConsoleLogger } from '@xyne/vespa-ts';
import type { VespaDependencies } from '@xyne/vespa-ts';
import { VespaSearchResponseToSearchResult } from '@xyne/vespa-ts/mappers';
import { scale } from '@xyne/vespa-ts/utils';

// Create a logger
const logger = new ConsoleLogger({ service: 'my-app' });

// Create dependencies
const dependencies: VespaDependencies = {
  logger,
  config: {
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
  },
  sourceSchemas: ['file', 'user', 'mail'],
  vespaEndpoint: 'http://localhost:8080'
};

// Create Vespa service
const vespaService = createVespaService(dependencies);

// Use the service
async function search(query: string) {
  try {
    const results = await vespaService.search(query);
    return VespaSearchResponseToSearchResult(results);
  } catch (error) {
    logger.error('Search failed:', error);
    throw error;
  }
}
```

## Benefits of Modular Imports

1. **Tree Shaking**: Only import what you need, reducing bundle size
2. **Better Organization**: Separate concerns into logical modules
3. **Type Safety**: Import only the types you need
4. **Flexibility**: Mix and match imports based on your use case

## Available Modules

- `@xyne/vespa-ts` - Core functions and main API
- `@xyne/vespa-ts/types` - TypeScript type definitions
- `@xyne/vespa-ts/mappers` - Data transformation functions
- `@xyne/vespa-ts/utils` - Utility functions
- `@xyne/vespa-ts/errors` - Custom error classes
- `@xyne/vespa-ts/client` - Client implementations

## Development

```bash
# Install dependencies
npm install

# Build the package
npm run build

# Watch for changes
npm run dev

# Run tests
npm test
```

## License

MIT
