from datetime import date
from typing import Optional


def interval_overlaps(
    a_start: date,
    a_end: Optional[date],
    b_start: date,
    b_end: Optional[date],
) -> bool:
    """
    Check if two date intervals overlap.
    
    Args:
        a_start: Start date of interval A (required)
        a_end: End date of interval A (None = open-ended)
        b_start: Start date of interval B (required)
        b_end: End date of interval B (None = open-ended)
    
    Returns:
        True if intervals overlap, False otherwise
    
    Examples:
        >>> from datetime import date
        >>> interval_overlaps(date(2020, 1, 1), date(2020, 12, 31), date(2020, 6, 1), date(2020, 6, 30))
        True
        >>> interval_overlaps(date(2020, 1, 1), date(2020, 6, 1), date(2020, 6, 2), date(2020, 12, 31))
        False
        >>> interval_overlaps(date(2020, 1, 1), None, date(2020, 6, 1), date(2020, 12, 31))
        True
        >>> interval_overlaps(date(2020, 1, 1), date(2020, 6, 1), date(2020, 6, 1), None)
        True
    """
    if a_end is None and b_end is None:
        return True
    
    if a_end is None:
        return a_start <= b_end if b_end else True
    
    if b_end is None:
        return b_start <= a_end
    
    return a_start <= b_end and b_start <= a_end


def parse_date_iso(date_str: Optional[str]) -> Optional[date]:
    """
    Parse ISO date string to date object.
    
    Args:
        date_str: ISO date string (YYYY-MM-DD) or None
    
    Returns:
        date object or None
    """
    if not date_str:
        return None
    try:
        return date.fromisoformat(date_str)
    except (ValueError, AttributeError):
        return None


def filter_mandates_by_overlap(
    mandates: list,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> list:
    """
    Filter mandates that overlap with the given date range.
    
    Args:
        mandates: List of mandate objects with start_date and end_date attributes
        from_date: Start date of filter range (None = no lower bound)
        to_date: End date of filter range (None = no upper bound)
    
    Returns:
        List of mandates that overlap with the date range
    """
    if from_date is None and to_date is None:
        return mandates
    
    result = []
    for mandate in mandates:
        mandate_start = parse_date_iso(getattr(mandate, "start_date", None))
        mandate_end = parse_date_iso(getattr(mandate, "end_date", None))
        
        if mandate_start is None:
            continue
        
        if from_date is None:
            filter_start = mandate_start
        else:
            filter_start = from_date
        
        if to_date is None:
            filter_end = mandate_end
        else:
            filter_end = to_date
        
        if interval_overlaps(mandate_start, mandate_end, filter_start, filter_end):
            result.append(mandate)
    
    return result

