import os
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Optional, Tuple, List


class FolderDateParser(ABC):
    """
    Abstract base class for parsing a date from a folder path.

    Subclasses must implement `parse_date_from_path()` to extract date information from directory structures.
    """
    @abstractmethod
    def parse_date_from_path(self, folder_path: str) -> Tuple[datetime, str]:
        pass
class YearMonthDayFolderDateParser(FolderDateParser):
    """
    Concrete implementation of FolderDateParser that expects folder paths in the format: YYYY/MM/DD.

    Example:
        For path '/data/wayfair/2025/06/30', it extracts datetime(2025, 6, 30).
    """

    def parse_date_from_path(self, path: str) -> Optional[Tuple[datetime, str]]:
        """
        Parses the last 3 parts of a path as year, month, and day.

        Args:
            path (str): Folder path string.

        Returns:
            Tuple of (datetime, path) if valid; otherwise None.
        """

        try:
            parts = path.strip(os.sep).split(os.sep)[-3:]  # Get last 3 parts: year/month/day
            if len(parts) != 3 or not all(p.isdigit() for p in parts):
                return None
            year, month, day = map(int, parts)
            date = datetime(year, month, day)
            return date, path
        except ValueError:
            return None
class LatestFolderFinder:
    """
    Finds the latest dated folder in a directory tree with structure: base_dir/YYYY/MM/DD.

    Uses a date parser (e.g. YearMonthDayFolderDateParser) to convert folder names into dates.
    """
    def __init__(self, base_dir: str, parser: FolderDateParser):
        self.base_dir = base_dir
        self.parser = parser

    def _gather_all_day_paths(self) -> List[str]:
        """
        Recursively collects all paths exactly 3 levels below base_dir (corresponding to YYYY/MM/DD).

        Returns:
            List of valid full paths.
        """

        paths = []
        for root, dirs, _ in os.walk(self.base_dir):
            depth = root[len(self.base_dir):].count(os.sep)
            if depth == 3:  # YYYY/MM/DD
                paths.append(root)
        return paths

    def find_latest(self) -> Optional[str]:
        """
        Finds the folder path with the latest date.

        Returns:
            Full path to the latest folder or None if no valid folders are found.
        """
        candidates = []
        for path in self._gather_all_day_paths():
            parsed = self.parser.parse_date_from_path(path)
            if parsed:
                candidates.append(parsed)

        if not candidates:
            return None

        # Get path with latest datetime
        latest_date, latest_path = max(candidates, key=lambda x: x[0])
        return latest_path

# wrapper
def get_latest_folder(base_dir: str) -> Optional[str]:
    """
    Convenience wrapper function to find the latest dated folder in the format: base_dir/YYYY/MM/DD.

    Args:
        base_dir (str): Base directory to search. eg: data/wayfair

    Returns:
        Path to the most recent folder, or None.
    """

    parser = YearMonthDayFolderDateParser()
    latest_folder = LatestFolderFinder(base_dir, parser)
    return latest_folder.find_latest()




