from datetime import datetime

def print_log(string_to_print: str) -> str:
    """
    Function to print on log format
    """
    return f"[{str(datetime.now())}] - {string_to_print}"
