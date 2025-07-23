def match(df, keywords):
    """
    Returns the best-matching column from df for any of the provided keywords.
    Uses case-insensitive partial token matching to handle variations.
    """
    candidates = [col for col in df.columns if isinstance(col, str)]
    lower_candidates = [col.lower() for col in candidates]

    for keyword in keywords:
        parts = keyword.lower().split()

        for i, candidate in enumerate(lower_candidates):
            if all(part in candidate for part in parts):
                return candidates[i]  # Return original column name

    return None  # No match found
