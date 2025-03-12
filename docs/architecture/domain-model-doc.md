# On Domain Model Coupling: a Smart Records Approach

## Introduction

When implementing domain-driven design, particularly with graph databases, striking the right balance between separation of concerns and practical usability presents challenges. This document outlines the Smart Record pattern as one possible approach to domain model design, examining its strengths and limitations.

## The Smart Record Pattern

The Smart Record pattern creates domain entities that know how to access their related data but delegate the actual data loading to a specialized component (the Repository).

```python
class Item:
    def __init__(self, uuid, title, repository=None):
        self.uuid = uuid
        self.title = title
        self._repository = repository
        self._files = None  # Lazy loaded

    @property
    def files(self) -> list[File]:
        if self._files is None and self._repository:
            self._files = self._repository.get_files_for_item(self.uuid)
        return self._files or []
```

This pattern differs from Active Record (where entities manage their own persistence) by maintaining a clearer separation of concerns: entities primarily hold data and business logic, while repositories handle persistence operations.

## Controlled Connection vs. Tight Coupling

Domain modeling approaches often gravitate toward either complete separation (anemic domain models with service layers) or tight coupling (entities with hard dependencies on persistence). 

The Smart Record approach creates a *controlled connection* - the Repository knows about Items, and Items have an awareness of a Repository interface without depending on specific implementations.

### Advantages
- Provides intuitive object-oriented API (e.g. `Item.files`)
- Maintains separation of persistence concerns
- Enables lazy loading for performance optimization

### Disadvantages
- Creates a circular reference that must be carefully managed
- Can blur the line between domain and persistence concerns
- May complicate testing if not properly designed

## Separation of Identity and Relationships

This design makes a distinction between:
- The core identity of an entity (UUID, title) - loaded eagerly
- Its relationships to other entities (files, etc.) - loaded lazily

This mirrors real-world object interactions but introduces complexity in managing object lifecycles.

## The Repository as Relationship Manager

_NOTE: pondering a `RelationshipManager` or something similar that would get injected into the Repository object to potentially read/write/parse relationships._

The Repository maintains its role as the authority on persistence and cross-domain relationships:

```python
# Repository handles persistence and relationships
repository.add_file(file, item_uuid)
repository.relate_items(source_uuid, relation, target_uuid)

# But domain models provide convenient access
item = repository.get_item(uuid)
for file in item.files:  # Lazy loading happens here
    print(file.name)
```

### Advantages
- Clear authority for persistence operations
- Cross-domain relationship management
- Clean object-oriented API for clients

### Disadvantages
- May create hidden performance issues with lazy loading
- Potential for inconsistent object states if relationships are modified externally

## Considerations for Graph Databases

This approach has specific implications for graph databases like Pyoxigraph:

### Advantages
- Natural mapping to graph concepts (nodes and edges)
- Support for lazy traversal of relationships
- Flexibility to add new relationship types
- Minimal initial schema requirements

### Disadvantages
- May not fully utilize graph database query capabilities
- Can create inefficient traversal patterns if not carefully designed
- Potential impedance mismatch between object model and graph model

## Practical Examples

The pattern creates code that reads naturally:

```python
item = repository.get_item("abc123")
print(f"Item: {item.title}")
print(f"Files: {[f.name for f in item.files]}")
```

However, hidden complexity exists in ensuring all relationships remain consistent, especially when multiple entities refer to the same related entities.

## Implementation Considerations

When implementing this pattern, several factors deserve attention:

1. **Repository Interface** - Keep it minimal in domain entities to reduce coupling
2. **Dependency Injection** - Consider making the Repository injectable but optional
3. **Property Design** - Use property decorators for clean access to lazy-loaded relationships
4. **Object Lifecycle** - Document the lifecycle clearly to prevent misuse
5. **Memory Management** - Consider weakrefs to prevent memory leaks in circular references
6. **Transaction Boundaries** - Define clear transaction boundaries for related operations

## Advanced Patterns

As systems grow in complexity, consider:

1. **Batch loading** - Load relationships for multiple entities at once
2. **Identity mapping** - Ensure consistency across the object graph
3. **Change tracking** - Monitor and batch persistence operations
4. **Query objects** - Separate complex query logic from both entities and repositories

## Conclusion

The Smart Record pattern offers a pragmatic approach to domain modeling that balances separation of concerns with usable APIs. It's particularly relevant for graph database applications but requires careful implementation to manage the inherent complexities of maintaining object relationships.

This pattern is not a silver bullet - it introduces a form of coupling that must be managed deliberately. However, for systems where relationship traversal is a common operation, it can provide a more intuitive developer experience than fully decoupled alternatives.