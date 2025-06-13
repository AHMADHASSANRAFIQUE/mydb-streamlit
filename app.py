import streamlit as st
import pandas as pd
import traceback
import re
from database import MyDB, Collection
from errors import DatabaseError, ValidationError, CollectionError

# Set page title
st.title("MyDB: Simple Database with Streamlit")

# Input validation functions
def validate_collection_name(name: str) -> bool:
    if not name or not re.match(r"^[a-zA-Z0-9_]{1,50}$", name):
        raise ValidationError("Collection name must be 1-50 characters, alphanumeric or underscore")
    return True

def validate_schema(schema: str) -> bool:
    if schema:
        fields = [s.strip() for s in schema.split(",")]
        for field in fields:
            if not field or not re.match(r"^[a-zA-Z0-9_]{1,50}$", field):
                raise ValidationError(f"Invalid field name: {field}. Must be 1-50 characters, alphanumeric or underscore")
            if field in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Field name '{field}' is reserved")
    return True

def validate_query(query: str) -> bool:
    if not query.strip():
        raise ValidationError("Query cannot be empty")
    return True

# Initialize Database in Session State
if 'db' not in st.session_state:
    try:
        st.session_state.db = MyDB()
        st.success("Database initialized successfully")
    except DatabaseError as e:
        st.error(f"Failed to initialize database: {e}")
        st.stop()

db = st.session_state.db

# Cache collection data
@st.cache_data
def get_collection_data(collection_name):
    try:
        validate_collection_name(collection_name)
        collection = db.collections.get(collection_name)
        if collection and collection.data:
            return pd.DataFrame(list(collection.data.values()))
        return None
    except ValidationError as e:
        st.error(f"Invalid collection name: {e}")
        return None

# Cache query results
@st.cache_data
def run_query(collection_name, query_str):
    try:
        validate_collection_name(collection_name)
        validate_query(query_str)
        collection = db.collections.get(collection_name)
        if collection:
            return collection.parse_query(query_str)
        raise CollectionError(f"Collection {collection_name} not found")
    except (ValidationError, CollectionError) as e:
        st.error(f"Query error: {e}")
        return []

# Sidebar for Collection Management
st.sidebar.header("Collection Management")
collection_name = st.sidebar.text_input("Collection Name", placeholder="e.g., student")
schema_input = st.sidebar.text_input("Schema (comma-separated fields, optional)", placeholder="e.g., name,roll_no,grade,age")
if st.sidebar.button("Create Collection"):
    try:
        validate_collection_name(collection_name)
        validate_schema(schema_input)
        schema = [s.strip() for s in schema_input.split(",")] if schema_input else []
        result = db.create_collection(collection_name, schema)
        st.sidebar.success(result)
    except (ValidationError, CollectionError) as e:
        st.sidebar.error(f"Failed to create collection: {e}")

# Select Collection
collection_names = list(db.collections.keys())
if not collection_names:
    st.write("No collections available. Create a collection to start.")
    selected_collection = None
else:
    selected_collection = st.sidebar.selectbox("Select Collection", collection_names)
collection = db.collections.get(selected_collection) if selected_collection else None

# Index Management
st.sidebar.header("Index Management")
index_fields = st.sidebar.text_input("Index Fields (comma-separated)", placeholder="e.g., name,age")
if st.sidebar.button("Create Index"):
    try:
        if not selected_collection:
            raise CollectionError("Select a collection first")
        if not index_fields:
            raise ValidationError("Index fields are required")
        fields = [f.strip() for f in index_fields.split(",")]
        for field in fields:
            if not re.match(r"^[a-zA-Z0-9_]{1,50}$", field):
                raise ValidationError(f"Invalid index field: {field}")
            if field in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Index field '{field}' is reserved")
        query = f"INDEX FIELD {','.join(fields)}"
        results = run_query(selected_collection, query)
        st.sidebar.success(f"Index created on {','.join(fields)}")
    except (ValidationError, CollectionError) as e:
        st.sidebar.error(f"Failed to create index: {e}")

# Main Interface
if collection:
    st.header(f"Collection: {selected_collection}")
    
    # Query Input
    st.subheader("Execute Query")
    query_examples = (
        "Examples:\n"
        "- Insert: ADD DATA (name='John', age=30)\n"
        "- Select: FETCH FILTER (name='John', age=30)\n"
        "- Update: MODIFY FILTER (name='John') WITH (age=31)\n"
        "- Delete: REMOVE FILTER (name='John', age=30)\n"
        "- Index: INDEX FIELD name,age\n"
        "- Aggregate: AGGREGATE (avg_age=$avg, count=$count) FILTER (grade='A') GROUP BY grade SORT BY avg_age:desc\n"
        "- Join: JOIN course ON roll_no=student_id FILTER (grade='A')"
    )
    query = st.text_input("Enter Query", placeholder=query_examples)
    if st.button("Run Query"):
        try:
            validate_query(query)
            results = run_query(selected_collection, query)
            if isinstance(results, dict) and "execution_time" in results:
                execution_time = results.pop("execution_time")
                st.write(f"Query execution time: {execution_time:.3f} seconds")
            if results:
                st.write("Query Results:")
                st.dataframe(pd.DataFrame(results))
            else:
                st.warning("No results returned")
        except (ValidationError, QueryError, CollectionError) as e:
            st.error(f"Query failed: {e}\n{traceback.format_exc()}")

    # Transaction Management
    st.subheader("Transaction Manager")
    tx_examples = (
        "Example:\n"
        "ADD DATA (name='Alice', age=25); MODIFY FILTER (name='Alice', age=25) WITH (age=26)"
    )
    tx_query = st.text_area("Enter Transaction Operations", placeholder=tx_examples)
    if st.button("Queue Transaction"):
        try:
            validate_query(tx_query)
            results = run_query(selected_collection, f"TRANSACT OPS ({tx_query})")
            if isinstance(results, dict) and "execution_time" in results:
                execution_time = results.pop("execution_time")
                st.write(f"Transaction execution time: {execution_time:.3f} seconds")
            st.success(f"Transaction: {results[0]['transaction']}")
        except (ValidationError, TransactionError, CollectionError) as e:
            st.error(f"Transaction failed: {e}\n{traceback.format_exc()}")

    # Display Collection Data
    st.subheader("Collection Data")
    df = get_collection_data(selected_collection)
    if df is not None:
        st.dataframe(df)
    else:
        st.write("No data in collection")

    # Display Indexes
    st.subheader("Indexes")
    if collection.indexes:
        for field, index in collection.indexes.items():
            st.write(f"Index on {field}: {index}")
    else:
        st.write("No indexes created")
else:
    if collection_names:
        st.write("Select a collection to start")
