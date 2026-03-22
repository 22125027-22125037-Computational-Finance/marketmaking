"""
QuickFIX configuration file generation utilities.

Provides helpers for generating QuickFIX .cfg files from minimal
parameters with sensible defaults for trading applications.
"""
from __future__ import annotations
from pathlib import Path
from importlib.resources import files, as_file
from typing import Optional, Union
import tempfile
import os
import shutil
import logging


logger = logging.getLogger(__name__)


# ---------- Public API ----------
def render_quickfix_cfg(
    *,
    socket_host: str,
    socket_port: Union[int, str],
    sender_comp_id: str,
    target_comp_id: str,
    file_store_path: str = "logs/client_fix_messages/",
    file_log_path: str = "logs/",
    relative_dd: bool = False,
    save_as: Optional[str] = None,
) -> str:
    """
    Generate QuickFIX configuration file from parameters.

    Creates a complete QuickFIX .cfg file with sensible defaults
    for FIX 4.4 initiator sessions. Handles data dictionary
    resolution and directory creation automatically.

    Args:
        socket_host: FIX server hostname or IP address.
        socket_port: FIX server port number.
        sender_comp_id: Your company/sender ID for FIX session.
        target_comp_id: Server/target company ID for FIX session.
        file_store_path: Directory for QuickFIX message store.
                        Defaults to "logs/client_fix_messages/".
        file_log_path: Directory for QuickFIX logs.
                      Defaults to "logs/".
        relative_dd: If True, copy FIX44.xml to ./fix/ and use
                    relative path. If False, use absolute path.
                    Defaults to False.
        save_as: If provided, save config to this path. Otherwise
                creates temporary file. Defaults to None.

    Returns:
        Absolute path to generated .cfg file.

    Raises:
        OSError: If directory creation or file writing fails.

    Example:
        >>> # Create temporary config
        >>> cfg_path = render_quickfix_cfg(
        ...     socket_host="fix.example.com",
        ...     socket_port=9876,
        ...     sender_comp_id="CLIENT",
        ...     target_comp_id="SERVER"
        ... )
        >>> print(cfg_path)
        /tmp/paperbroker_abc123.cfg

        >>> # Save to specific path
        >>> cfg_path = render_quickfix_cfg(
        ...     socket_host="fix.example.com",
        ...     socket_port=9876,
        ...     sender_comp_id="CLIENT",
        ...     target_comp_id="SERVER",
        ...     save_as="config/live.cfg"
        ... )

    Note:
        - Creates file_store_path and file_log_path if they don't exist
        - Temporary files are not automatically cleaned up
        - Uses FIX 4.4 protocol with standard data dictionary
    """
    dd_value = _resolve_data_dictionary(relative_dd=relative_dd)
    cfg_text = _build_cfg_text(
        socket_host=str(socket_host),
        socket_port=str(socket_port),
        sender_comp_id=sender_comp_id,
        target_comp_id=target_comp_id,
        file_store_path=file_store_path,
        file_log_path=file_log_path,
        data_dictionary=dd_value,
    )
    _ensure_dirs(file_store_path, file_log_path)

    if save_as:
        path = Path(save_as).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(cfg_text, encoding="utf-8")
        logger.info(f"Saved QuickFIX config to: {path}")
        return str(path)
    else:
        fd, tmp_cfg = tempfile.mkstemp(prefix="paperbroker_", suffix=".cfg")
        os.close(fd)
        Path(tmp_cfg).write_text(cfg_text, encoding="utf-8")
        logger.debug(f"Created temporary QuickFIX config: {tmp_cfg}")
        return tmp_cfg


# ---------- Internals ----------
def _resolve_data_dictionary(*, relative_dd: bool) -> str:
    """
    Resolve FIX data dictionary file path.

    Returns the path to FIX44.xml data dictionary, either as
    absolute path (safe for wheel/zip installations) or by
    copying to local ./fix/ directory.

    Args:
        relative_dd: If True, copy to ./fix/FIX44.xml and return
                    relative path. If False, return absolute path.

    Returns:
        Path to FIX44.xml (absolute or relative).

    Note:
        Uses importlib.resources for proper package resource access.
    """
    res = files("paperbroker").joinpath("resources/FIX44.xml")
    if relative_dd:
        dst_dir = Path("fix")
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "FIX44.xml"
        # as_file gives us a real path even if running from a wheel/zip
        with as_file(res) as src_path:
            shutil.copyfile(src_path, dst)
        logger.debug(f"Copied FIX44.xml to: {dst}")
        return "fix/FIX44.xml"
    else:
        with as_file(res) as abs_path:
            logger.debug(f"Using absolute FIX44.xml path: {abs_path}")
            return str(abs_path)


def _build_cfg_text(
    *,
    socket_host: str,
    socket_port: str,
    sender_comp_id: str,
    target_comp_id: str,
    file_store_path: str,
    file_log_path: str,
    data_dictionary: str,
) -> str:
    """
    Build QuickFIX configuration file content.

    Internal function that generates the actual .cfg file text
    with DEFAULT and SESSION sections.

    Args:
        socket_host: FIX server hostname.
        socket_port: FIX server port.
        sender_comp_id: Client company ID.
        target_comp_id: Server company ID.
        file_store_path: Message store directory.
        file_log_path: Log file directory.
        data_dictionary: Path to FIX44.xml.

    Returns:
        Complete .cfg file content as string.

    Note:
        Configuration includes:
        - 30 second heartbeat interval
        - 24-hour session window (00:00:00 - 23:59:59)
        - Reset on logon/logout/disconnect
        - Sequence number tolerance settings
    """
    # Keep your desired defaults here
    default_block = f"""[DEFAULT]
ConnectionType=initiator
SocketConnectHost={socket_host}
SocketConnectPort={socket_port}
StartTime=00:00:00
EndTime=23:59:59
HeartBtInt=30
FileStorePath={file_store_path}
FileLogPath={file_log_path}
""".rstrip()

    session_block = f"""

[SESSION]
BeginString=FIX.4.4
SenderCompID={sender_comp_id}
TargetCompID={target_comp_id}
DataDictionary={data_dictionary}
ResetOnLogon=Y
ResetOnLogout=Y
ResetOnDisconnect=Y
IgnoreSeqNumTooLow=Y
ValidateFieldsOutOfOrder=N
""".rstrip()

    return default_block + session_block


def _ensure_dirs(store_path: str, log_path: str) -> None:
    """
    Create required directories if they don't exist.

    Args:
        store_path: Message store directory path.
        log_path: Log file directory path.

    Note:
        Creates parent directories as needed (parents=True).
    """
    Path(store_path).mkdir(parents=True, exist_ok=True)
    Path(log_path).mkdir(parents=True, exist_ok=True)
    logger.debug(
        f"Ensured directories exist: {store_path}, {log_path}"
    )
