from ingestion_lambda import lambda_handler

def check_table_schema(table_name):
    connection = connect_to_warehouse()
    
    try:
        schema_query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable,
            ordinal_position
        FROM information_schema.columns 
        WHERE table_schema = 'public' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        
        schema_df = wr.postgresql.read_sql_query(sql=schema_query, con=connection)
        
        if schema_df.empty:
            logger.info(f"Table {table_name} not found or has no columns")
            return
        
        logger.info(f"Schema for {table_name}:")
        logger.info(f"\n{schema_df.to_string(index=False)}")
        
        return schema_df
        
    except Exception as e:
        logger.error(f"Failed to check table schema: {e}")
        raise
    finally:
        connection.close()