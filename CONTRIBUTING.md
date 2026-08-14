# Contributing

This document outlines the process and guidelines for contributing.

## How to Contribute

### Reporting Issues

- Check existing issues before creating a new one
- Use a clear, descriptive title
- Include Go version, OS, `cq` version, and `cq-dashboard` version
- Provide minimal reproducible code examples
- Describe expected vs actual behavior

### Suggesting Features

- Open an issue
- Explain the use case and why it's valuable
- Consider how it fits the store/sink/web split, and whether it belongs
  here rather than in `cq` itself
- Discuss API design before implementing

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run tests and ensure they pass
6. Update documentation, if needed
7. Commit with descriptive messaging
8. Push to your fork and open a pull request

## Development Setup

### Prerequisites

- Go 1.25 or higher (required by `modernc.org/sqlite`)
- make (optional, for convenience commands)

### Clone and Setup

```bash
git clone https://github.com/gnikyt/cq-dashboard.git
cd cq-dashboard
go mod download
```

### Running Tests

```bash
# Run all tests
make test

# Or use go test directly
go test ./... -v -race -cover

# Run the demo, which generates mixed traffic through every view
go run ./cmd/demo -addr :8080
```

### Project Structure

```
.
├── README.md      # Usage, boundaries, authentication
├── DESIGN.md      # Durable visual and UX decisions
├── store/         # Persistence contract, plus the SQLite implementation
├── sink/          # cq hooks to stored history, without blocking a worker
├── web/           # Handler, templates, auth, controls
├── cmd/demo/      # Worked example generating realistic traffic
└── example/       # Compiles the README's wiring, so the docs cannot rot
```

## Code Guidelines

### Go Conventions

- Follow standard Go formatting (`gofmt`, `goimports`)
- Write idiomatic Go code
- Keep exported APIs minimal and clear
- Use meaningful variable and function names
- Add doc comments for all exported types and functions

### Boundaries

This module observes cq, it does not extend it:

1. Never block a cq worker. Hooks hand work to the buffered writer and return
2. Shed load rather than slow the queue, and count what was shed
3. Writes must be order-tolerant... cq can report a start before an enqueue
4. Anything that belongs in the engine belongs in `cq`, not here
5. Keep the core dependency-light: no build step, no framework

### Adding a database backend

`store.Store` is the only seam: the sink and the web layer never see a driver.
A new backend is one package that satisfies the interface, and it is not done
until it passes the shared conformance suite:

```go
func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T) store.Store {
		// Return a fresh, migrated, empty store. Clean up with t.Cleanup.
	})
}
```

The suite is the specification. It pins the semantics that are easy to get
subtly wrong and impossible to notice by eye: out-of-order events merging
without regressing state, epoch isolation for cq's per-process job IDs,
lineage scoped to one epoch, prune never touching unfinished work, and a
frozen `Before` window for pagination.

### Templates

Template errors only surface at execution, so every view needs a rendering
test. `web/handler_test.go` renders each page against a real queue and store...
add to it rather than trusting a template by eye.

### Testing

- Write tests for new features and bug fixes
- Aim for >90% test coverage
- Include edge cases and error paths
- Use table-driven tests where appropriate
- Test wrapper composition behavior

Example test structure:

```go
func TestWithExample(t *testing.T) {
	t.Run("success_case", func(t *testing.T) {
		// Test implementation
	})
	
	t.Run("error_case", func(t *testing.T) {
		// Test implementation
	})
}
```

### Documentation

- Update `README.md` if changing setup, authentication, or boundaries
- Update `DESIGN.md` when changing durable visual or UX rules
- Update `example/integration_test.go` when the wiring changes, so the
  documented setup keeps compiling
- Include code examples that demonstrate usage
- Document edge cases and caveats in wrapper docs
- Keep lines to 80 column, if possible (excluding code blocks)
- Use the standardized subsection format:
  - **What it does**
  - **When to use**
  - **Example**
  - **Caveat**

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass (`make test`)
- [ ] Code is formatted (`gofmt`)
- [ ] New features have tests
- [ ] Documentation is updated
- [ ] Benchmarks added for performance-sensitive code
- [ ] Commit messages are clear and descriptive

### PR Description

Include:
- Summary of changes
- Motivation and context
- Related issue numbers, if applicable
- Breaking changes, if any
- How to test the changes

### Review Process

- We will review your PR
- Address feedback and update as needed
- Once approved, we will merge as squashed

## Code of Conduct

- Be respectful and constructive
- Welcome newcomers and help them contribute
- Focus on what is best for the project
- Show empathy towards other contributors

## Questions?

- Open a discussion in GitHub Discussions
- Ask in an existing issue if relevant
- Reach out to maintainers if needed

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
