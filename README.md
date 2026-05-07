# @team-deepiri/shared-utils

Shared utilities package for Deepiri microservices.

## Overview

This package provides common utilities that are used across multiple microservices, reducing code duplication and ensuring consistency.

## Installation

```bash
cd platform-services/shared/deepiri-shared-utils
npm install
```

## Usage

### Logger

The logger utility provides consistent logging across all services:

```javascript
const { createLogger } = require('@team-deepiri/shared-utils');

// Create a logger for your service
const logger = createLogger('my-service');

// Use the logger
secureLog('info', 'Service started');
secureLog('error', 'An error occurred', { error: err });
secureLog('warn', 'Warning message');
logger.debug('Debug information');
```

### Redis Streams ACK Handling

`StreamingClient.subscribe()` acknowledges consumer-group messages after the
callback finishes, even when the callback throws. This prevents a bad handler
from leaving messages pinned in the Redis Pending Entry List forever.

ACK errors are still visible and classified:

- `XACK = 0`: Redis accepted the command, but the message was not pending for
  that group anymore.
- Retryable Redis/transport states: connection resets, closed connections,
  `LOADING`, `TRYAGAIN`, `CLUSTERDOWN`, `READONLY`, and timeouts.
- Non-retryable setup bugs: `NOGROUP` and `WRONGTYPE`.

This keeps consumer loops alive while preserving enough stream/group/message
context to investigate the underlying Redis issue.

### Service-Specific Loggers

Each service also has its own logger in `services/{service-name}/utils/logger.js`:

```javascript
const logger = require('../../utils/logger');
secureLog('info', 'Service-specific log message');
```

## Structure

```
deepiri-shared-utils/
├── logger.js      # Logger factory function
├── index.js      # Main entry point
├── package.json   # Package configuration
└── README.md      # This file
```

## Future Utilities

Additional shared utilities can be added to this package:

- Error handling utilities
- Validation helpers
- Common middleware
- Database connection helpers
- API client utilities

## Development

To add new utilities:

1. Create a new file in the `shared-utils` directory
2. Export the utility from `index.js`
3. Update this README with usage examples
4. Update the package version

## Integration with Services

Services can use this package in two ways:

1. **As a local package** (current approach): Copy utilities to each service
2. **As an npm package** (future): Publish to npm or use as a workspace dependency

For now, each service has its own copy of the logger utility in `utils/logger.js` to ensure Docker builds work correctly.

