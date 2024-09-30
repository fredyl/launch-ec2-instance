complex_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (ArrayType, MapType))]
