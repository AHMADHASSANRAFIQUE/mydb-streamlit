class DatabaseError(Exception):
    """Base exception for database-related errors."""
    pass

class ValidationError(DatabaseError):
    """Raised when validation fails for inputs or records."""
    pass

class CollectionError(DatabaseError):
    """Raised when collection operations fail."""
    pass

class QueryError(DatabaseError):
    """Raised when query parsing or execution fails."""
    pass

class TransactionError(DatabaseError):
    """Raised when transaction operations fail."""
    pass

class IndexError(DatabaseError):
    """Raised when index operations fail."""
    pass
