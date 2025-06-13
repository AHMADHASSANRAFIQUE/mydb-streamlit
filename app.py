import streamlit as st
import pandas as pd
import traceback

try:
    from database import MyDB, Collection
except ImportError as e:
    st.error(f"Failed to import database module: {e}")
    st.write("Ensure all required files (__init__.py, mydb_types.py, query.py, queryParser.py, index.py, transaction.py, database.py) are in the same directory as app.py.")
    st.stop()

# Set page title
st.title("MyDB: Simple Database with Streamlit")

# Initialize Database in Session State
if 'db' not in st.session_state:
    try:
        st.session_state.db = MyDB()
        st.success("Database initialized successfully")
    except Exception as e:
        st.error(f"Failed to initialize database: {e}")
        st.stop()

db = st.session_state.db

# Cache collection data
@st.cache_data
def get_collection_data(collection_name):
    collection = db.collections.get(collection_name)
    if collection and collection.data:
        return pd.DataFrame(list(collection.data.values()))
    return None

# Cache query results
@st.cache_data
def run_query(collection_name, query_str):
    collection = db.collections.get(collection_name)
    if collection:
        return collection.parse_query(query_str)
    return []

# Sidebar for Collection Management
st.sidebar.header("Collection Management")
collection_name = st.sidebar.text_input("Collection Name", placeholder="e.g., student")
schema_input = st.sidebar.text_input("Schema (comma-separated fields, optional)", placeholder="e.g., name,roll_no,grade,age")
if st.sidebar.button("Create Collection"):
    if not collection_name:
        st.sidebar.error("Collection name is required")
    else:
        try:
            schema = [s.strip() for s in schema_input.split(",")] if schema_input else []
            result = db.create_collection(collection_name, schema)
            st.sidebar.success(result)
        except Exception as e:
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
    if not selected_collection:
        st.sidebar.error("Select a collection first")
    elif not index_fields:
        st.sidebar.error("Index fields are required")
    else:
        try:
            fields = [f.strip() for f in index_fields.split(",")]
            query = f"INDEX FIELD {','.join(fields)}"
            results = run_query(selected_collection, query)
            st.sidebar.success(f"Index created on {','.join(fields)}")
        except Exception as e:
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
        if not query:
            st.error("Query cannot be empty")
        else:
            try:
                results = run_query(selected_collection, query)
                if isinstance(results, dict) and "execution_time" in results:
                    execution_time = results.pop("execution_time")
                    st.write(f"Query execution time: {execution_time:.3f} seconds")
                if results:
                    st.write("Query Results:")
                    st.dataframe(pd.DataFrame(results))
                else:
                    st.warning("No results returned")
            except Exception as e:
                st.error(f"Query failed: {e}\n{traceback.format_exc()}")

    # Transaction Management
    st.subheader("Transaction Manager")
    tx_examples = (
        "Example:\n"
        "ADD DATA (name='Alice', age=25); MODIFY FILTER (name='Alice', age=25) WITH (age=26)"
    )
    tx_query = st.text_area("Enter Transaction Operations", placeholder=tx_examples)
    if st.button("Queue Transaction"):
        if not tx_query:
            st.error("Transaction operations cannot be empty")
        else:
            try:
                results = run_query(selected_collection, f"TRANSACT OPS ({tx_query})")
                if isinstance(results, dict) and "execution_time" in results:
                    execution_time = results.pop("execution_time")
                    st.write(f"Transaction execution time: {execution_time:.3f} seconds")
                st.success(f"Transaction: {results[0]['transaction']}")
            except Exception as e:
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