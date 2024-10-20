from unidecode import unidecode

def normalize_column_names(list_of_columns):
    normalized_columns = {}
    for col in list_of_columns:
        normalized_columns[col] = normalize_string_to_column_name(col)
    return normalized_columns
        
        
def normalize_string_to_column_name(column):
    normalized_column = unidecode(column).replace(" ", "_") \
        .replace("~", "") \
        .replace("?", "") \
        .replace("+", "plus") \
        .replace("&", "n") \
        .replace(":", "") \
        .replace("'", "") \
        .replace("`", "") \
        .replace("(", "") \
        .replace(")", "") \
        .replace("-", "_") \
        .replace("/", "_") \
        .replace("-", "_") \
        .lower()
    return normalized_column