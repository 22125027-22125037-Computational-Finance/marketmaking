"""
Structured logging utilities for paperbroker-client.

Provides custom logger with FIX protocol awareness, structured
logging support, and colorized console output.
"""
import logging
import os
from datetime import datetime


# Reserved LogRecord attributes that should not be treated as extras
_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
}


# ANSI color codes for terminal output
class Colors:
    """ANSI color codes for terminal output."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    # Foreground colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Bright foreground colors
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    
    # Background colors
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'


# Log level to color mapping
LEVEL_COLORS = {
    'DEBUG': Colors.CYAN,
    'INFO': Colors.GREEN,
    'WARNING': Colors.YELLOW,
    'ERROR': Colors.RED,
    'CRITICAL': Colors.BG_RED + Colors.WHITE + Colors.BOLD,
}


class FixFormatter(logging.Formatter):
    """
    Custom log formatter with FIX protocol support.

    Formats log records with FIX SOH (Start of Header) character
    replacement and structured extra fields rendering.

    Features:
        - Replaces FIX SOH (0x01) with '|' for readability
        - Renders extra fields as key=value pairs
        - Preserves standard logging format
        - Thread-safe operation

    Example:
        >>> formatter = FixFormatter("[%(asctime)s] %(message)s")
        >>> handler.setFormatter(formatter)
        >>>
        >>> # Log with extras
        >>> logger.info(
        ...     "Order placed",
        ...     extra={"order_id": "123", "symbol": "AAPL"}
        ... )
        >>> # Output: [2024-01-15 10:30:00] Order placed
        >>> #         order_id=123 symbol=AAPL

    Note:
        FIX messages use SOH (0x01) as field delimiter. This formatter
        replaces it with '|' for better log readability.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with FIX awareness and extras.

        Processes the log record by:
        1. Replacing FIX SOH characters with '|'
        2. Applying standard formatting
        3. Appending extra fields as key=value pairs

        Args:
            record: LogRecord to format.

        Returns:
            Formatted log message string.

        Thread Safety:
            Safe to call from multiple threads. LogRecord instances
            are thread-local.

        Example:
            >>> record = logger.makeRecord(
            ...     "paperbroker", logging.INFO, "app.py", 10,
            ...     "Test\x01message", (), None,
            ...     extra={"user": "trader"}
            ... )
            >>> formatted = formatter.format(record)
            >>> print(formatted)
            [2024-01-15 10:30:00] [INFO] Test|message user=trader
        """
        # Replace FIX SOH with pipe for readability
        if isinstance(record.msg, str):
            record.msg = record.msg.replace("\x01", "|")
        
        # Apply standard formatting
        base = super().format(record)
        
        # Collect extra fields (non-reserved, non-private)
        extras = " ".join(
            f"{k}={record.__dict__[k]}"
            for k in record.__dict__
            if k not in _RESERVED and not k.startswith("_")
        )
        
        # Append extras if any
        return f"{base} {extras}" if extras else base


class ColoredFormatter(FixFormatter):
    """
    Colorized formatter for console output with FIX support.
    
    Extends FixFormatter with ANSI color codes for different log levels.
    
    Features:
        - Color-coded log levels (DEBUG=cyan, INFO=green, etc.)
        - All features from FixFormatter
        - Automatic color reset after each message
    
    Example:
        >>> formatter = ColoredFormatter("[%(asctime)s] %(message)s")
        >>> console_handler.setFormatter(formatter)
        >>> logger.info("This will be green")
        >>> logger.error("This will be red")
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format with colors based on log level."""
        # Get base formatted message
        message = super().format(record)
        
        # Add color based on level
        color = LEVEL_COLORS.get(record.levelname, '')
        if color:
            # Colorize the level name in the message
            level = record.levelname
            colored_level = f"{color}{level}{Colors.RESET}"
            message = message.replace(f"[{level}]", f"[{colored_level}]")
        
        return message


def get_logger(
    log_dir: str = "logs",
    console: bool = True
) -> logging.Logger:
    """
    Get or create paperbroker logger instance.

    Creates a configured logger with file and console handlers.
    Uses colorized output for console and plain text for files.

    Args:
        log_dir: Directory for log files. Created if doesn't exist.
                Defaults to "logs".
        console: Log level control for console output:
                - True: Show DEBUG and above (all logs)
                - False: Show WARNING and above only (hide DEBUG/INFO)
                Defaults to True.

    Returns:
        Configured Logger instance named "paperbroker".

    Features:
        - Daily log rotation (new file per day)
        - UTF-8 encoding for international symbols
        - Always logs DEBUG+ to file
        - Console level controlled by console parameter
        - Colorized console output for better readability
        - FIX protocol awareness (SOH character handling)
        - Structured extra fields support

    Example:
        >>> # Development mode: see all logs in console
        >>> logger = get_logger(console=True)
        >>> logger.debug("This shows in console and file")
        >>> logger.info("This shows in console and file")
        >>>
        >>> # Production mode: only warnings/errors in console
        >>> logger = get_logger(console=False)
        >>> logger.debug("Only in file, not in console")
        >>> logger.info("Only in file, not in console")
        >>> logger.warning("Shows in BOTH console and file")
        >>> logger.error("Shows in BOTH console and file")

    Thread Safety:
        Safe to call from multiple threads. Returns same logger
        instance for same logger name.

    Note:
        - Log files: paperbroker_YYYYMMDD.log (always DEBUG level)
        - Console: colorized, level based on console parameter
        - All paperbroker.* child loggers inherit this configuration
    """
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate daily log file name
    log_file = os.path.join(
        log_dir, f"paperbroker_{datetime.now().strftime('%Y%m%d')}.log"
    )

    # Get or create logger
    logger = logging.getLogger("paperbroker")
    logger.setLevel(logging.DEBUG)  # Logger itself accepts all levels
    logger.propagate = False  # Don't propagate to root logger

    # Clear existing handlers and reconfigure
    # This allows changing console level when get_logger() is called again
    logger.handlers.clear()
    
    # File handler - ALWAYS logs everything (DEBUG+)
    file_formatter = FixFormatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)  # File gets everything
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)

    # Console handler - Level depends on console parameter
    console_formatter = ColoredFormatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch = logging.StreamHandler()
    if console:
        # Development mode: show all logs
        ch.setLevel(logging.DEBUG)
    else:
        # Production mode: only show warnings and errors
        ch.setLevel(logging.WARNING)
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)
    
    # Configure all child loggers to propagate to parent
    # This ensures consistent behavior across the package
    _configure_child_loggers()

    return logger


def _configure_child_loggers():
    """
    Configure all paperbroker child loggers to propagate to parent.
    
    This ensures child loggers don't create their own handlers
    and instead propagate all log records to the parent "paperbroker"
    logger, which controls file and console output centrally.
    
    Note:
        Called automatically by get_logger(). Child loggers must use
        logging.getLogger(__name__) where __name__ starts with "paperbroker."
    """
    # List of known child logger names in the package
    child_logger_names = [
        'paperbroker.session.app',
        'paperbroker.session.handler_admin',
        'paperbroker.session.handler_app',
        'paperbroker.session.handler_logon',
        'paperbroker.session.session_manager',
        'paperbroker.session.OrderManager',
        'paperbroker.rest.rest_session',
        'paperbroker.rest.account_client',
        'paperbroker.events',
        'paperbroker.config',
        'paperbroker.sub_account',
        'paperbroker.market_data.client',
        'paperbroker.market_data.kafka_provider',
    ]
    
    for logger_name in child_logger_names:
        child_logger = logging.getLogger(logger_name)
        child_logger.setLevel(logging.DEBUG)
        # Propagate to parent "paperbroker" logger
        child_logger.propagate = True
        # Clear any existing handlers
        child_logger.handlers.clear()
