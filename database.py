#!/usr/bin/env python3
"""
iMessage Database Builder
Converts iMessage export to SQLite database
"""

import sqlite3
import os
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging


class MessageDatabaseBuilder:
    def __init__(self, export_dir: str, db_path: str):
        self.export_dir = Path(export_dir)
        self.db_path = db_path
        self.connection = None

        # Compile regex patterns for performance
        self.timestamp_pattern = re.compile(
            r'(\w+ \d{1,2}, \d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))'
        )
        self.read_receipt_pattern = re.compile(
            r'\(Read by (them|you) after (.+?)\)'
        )
        self.edited_pattern = re.compile(
            r'Edited (\d+) (?:second|minute|hour)s? later:\s*(.+)'
        )
        self.unsent_pattern = re.compile(r'(.+?) unsent a message!')
        self.attachment_pattern = re.compile(
            r'attachments[/\\]([\d]+)[/\\]([\w\s.-]+\.\w+)',
            re.IGNORECASE
        )

        # Cache for attachment file lookups
        self.attachment_cache: Dict[str, Tuple[str, int, str]] = {}

        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)

    def create_database_schema(self):
        """Create database schema"""
        self.logger.info(f"Creating database: {self.db_path}")

        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        self.connection = sqlite3.connect(self.db_path)
        # Performance optimizations
        self.connection.execute('PRAGMA journal_mode = WAL')
        self.connection.execute('PRAGMA synchronous = NORMAL')
        self.connection.execute('PRAGMA cache_size = 10000')
        self.connection.execute('PRAGMA temp_store = MEMORY')

        cursor = self.connection.cursor()

        # Conversations table
        cursor.execute('''
            CREATE TABLE conversations (
                target_number TEXT PRIMARY KEY,
                display_name TEXT,
                message_count INTEGER DEFAULT 0,
                attachment_count INTEGER DEFAULT 0,
                first_message_date TEXT,
                last_message_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Messages table
        cursor.execute('''
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_number TEXT NOT NULL,
                message_date TEXT,
                sender TEXT,
                message_text TEXT,
                is_from_me BOOLEAN,
                reply_to_message_id INTEGER,
                message_type TEXT DEFAULT 'text',
                indent_level INTEGER DEFAULT 0,
                read_receipt_info TEXT,
                edited_text TEXT,
                edit_timestamp TEXT,
                is_unsent BOOLEAN DEFAULT FALSE,
                expressive_type TEXT,
                special_data TEXT,
                line_number INTEGER,
                is_duplicate BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (target_number) REFERENCES conversations (target_number),
                FOREIGN KEY (reply_to_message_id) REFERENCES messages (id)
            )
        ''')

        # Attachments table
        cursor.execute('''
            CREATE TABLE attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                target_number TEXT NOT NULL,
                filename TEXT,
                relative_path TEXT,
                directory_number TEXT,
                file_size INTEGER,
                mime_type TEXT,
                is_sticker BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (message_id) REFERENCES messages (id),
                FOREIGN KEY (target_number) REFERENCES conversations (target_number)
            )
        ''')

        # Tapbacks table
        cursor.execute('''
            CREATE TABLE tapbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_message_id INTEGER NOT NULL,
                target_number TEXT NOT NULL,
                sender TEXT,
                tapback_type TEXT,
                is_from_me BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (target_message_id) REFERENCES messages (id),
                FOREIGN KEY (target_number) REFERENCES conversations (target_number)
            )
        ''')

        # Create indexes for performance
        cursor.execute('CREATE INDEX idx_messages_target_number ON messages(target_number)')
        cursor.execute('CREATE INDEX idx_messages_date ON messages(message_date)')
        cursor.execute('CREATE INDEX idx_messages_sender ON messages(sender)')
        cursor.execute('CREATE INDEX idx_messages_is_from_me ON messages(is_from_me)')
        cursor.execute('CREATE INDEX idx_messages_reply_to ON messages(reply_to_message_id)')
        cursor.execute('CREATE INDEX idx_messages_is_duplicate ON messages(is_duplicate)')
        cursor.execute('CREATE INDEX idx_attachments_message_id ON attachments(message_id)')
        cursor.execute('CREATE INDEX idx_attachments_target_number ON attachments(target_number)')

        self.connection.commit()
        self.logger.info("Database schema created")

    def build_attachment_cache(self, conv_dir: Path):
        """Build cache of attachment files for faster lookup"""
        attachments_dir = conv_dir / "attachments"

        if not attachments_dir.exists():
            return

        self.logger.info(f"  Building attachment cache")

        for root, dirs, files in os.walk(attachments_dir):
            for file in files:
                if not file.startswith('.'):
                    full_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(full_path)
                        rel_path = os.path.relpath(full_path, self.export_dir)
                        # Extract directory number from path
                        dir_match = re.search(r'attachments[/\\]([\d]+)[/\\]', rel_path)
                        dir_number = dir_match.group(1) if dir_match else None
                        self.attachment_cache[file] = (rel_path, file_size, dir_number)
                    except OSError:
                        continue

    def parse_conversation_file(self, txt_file: Path, target_number: str) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Parse conversation file into messages, attachments, and tapbacks"""
        self.logger.info(f"  Parsing {txt_file.name}")

        with open(txt_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        messages = []
        attachments = []
        tapbacks = []

        current_message = None
        in_tapbacks_section = False
        reply_stack = []
        seen_message_signatures = set()

        for line_num, line in enumerate(lines):
            line = line.rstrip('\n\r')

            if not line.strip():
                in_tapbacks_section = False
                continue

            # Detect indentation level for reply threading
            indent_level = 0
            stripped_line = line.lstrip(' ')
            if line != stripped_line:
                indent_spaces = len(line) - len(stripped_line)
                indent_level = indent_spaces // 4

            # Check for tapbacks section
            if stripped_line.strip() == "Tapbacks:":
                in_tapbacks_section = True
                continue

            if in_tapbacks_section:
                if current_message:
                    tapback = self._parse_tapback_line(stripped_line, target_number)
                    if tapback:
                        tapback['target_message_line'] = current_message['line_number']
                        tapbacks.append(tapback)
                continue

            # Check for duplicate marker
            if "This message responded to an earlier message" in stripped_line:
                if current_message:
                    current_message['is_duplicate'] = True
                continue

            # Check for unsent message pattern
            unsent_match = self.unsent_pattern.search(stripped_line)
            if unsent_match:
                sender = unsent_match.group(1)
                if current_message:
                    messages.append(current_message)

                current_message = {
                    'target_number': target_number,
                    'message_date': '',
                    'sender': sender,
                    'message_text': '[Message unsent]',
                    'is_from_me': sender == "Me",
                    'reply_to_message_id': None,
                    'message_type': 'unsent',
                    'indent_level': indent_level,
                    'is_unsent': True,
                    'line_number': line_num,
                    'is_duplicate': False,
                    'attachments': [],
                    'read_receipt_info': None,
                    'edited_text': None,
                    'edit_timestamp': None,
                    'expressive_type': None
                }
                continue

            # Check for timestamp
            timestamp_match = self.timestamp_pattern.search(stripped_line)
            if timestamp_match:
                # Save previous message
                if current_message:
                    messages.append(current_message)

                # Extract read receipt info
                read_receipt = None
                receipt_match = self.read_receipt_pattern.search(stripped_line)
                if receipt_match:
                    read_receipt = {
                        'read_by': receipt_match.group(1),
                        'duration': receipt_match.group(2)
                    }

                # Check for expressive effects
                expressive_type = None
                if 'Sent with' in stripped_line:
                    expressive_match = re.search(r'Sent with (.+)', stripped_line)
                    if expressive_match:
                        expressive_type = expressive_match.group(1)

                # Determine parent for replies
                reply_to_id = None
                if indent_level > 0:
                    while len(reply_stack) > indent_level:
                        reply_stack.pop()
                    if reply_stack:
                        reply_to_id = reply_stack[-1]

                current_message = {
                    'target_number': target_number,
                    'message_date': timestamp_match.group(1),
                    'sender': None,
                    'message_text': '',
                    'text_parts': [],
                    'is_from_me': False,
                    'reply_to_message_id': reply_to_id,
                    'message_type': 'text',
                    'indent_level': indent_level,
                    'is_unsent': False,
                    'line_number': line_num,
                    'is_duplicate': False,
                    'attachments': [],
                    'read_receipt_info': json.dumps(read_receipt) if read_receipt else None,
                    'edited_text': None,
                    'edit_timestamp': None,
                    'expressive_type': expressive_type
                }
                continue

            # Check if this is a sender line (comes right after timestamp)
            if current_message and current_message['sender'] is None:
                sender = stripped_line.strip()
                current_message['sender'] = sender
                current_message['is_from_me'] = sender == "Me"

                # Add to reply stack for threading
                if len(reply_stack) <= indent_level:
                    reply_stack.extend([None] * (indent_level + 1 - len(reply_stack)))
                reply_stack[indent_level] = line_num
                continue

            # Check for edited message
            edited_match = self.edited_pattern.search(stripped_line)
            if edited_match and current_message:
                current_message['edited_text'] = edited_match.group(2)
                current_message['edit_timestamp'] = edited_match.group(1)
                continue

            # Check for attachments
            attachment_match = self.attachment_pattern.search(stripped_line)
            if attachment_match and current_message:
                dir_num = attachment_match.group(1)
                filename = attachment_match.group(2)

                attachment_info = {
                    'filename': filename,
                    'directory_number': dir_num,
                    'is_sticker': 'Sticker' in line,
                    'line_content': stripped_line
                }
                current_message['attachments'].append(attachment_info)
                continue

            # Regular message text
            if current_message and current_message['sender']:
                current_message['text_parts'].append(stripped_line)

        # Add the last message
        if current_message:
            messages.append(current_message)

        # Post-process messages
        for msg in messages:
            if 'text_parts' in msg:
                msg['message_text'] = '\n'.join(msg['text_parts']).strip()
                del msg['text_parts']

            # Create signature for duplicate detection
            signature = f"{msg['message_date']}|{msg['sender']}|{msg['message_text'][:100]}"
            if signature in seen_message_signatures:
                msg['is_duplicate'] = True
            else:
                seen_message_signatures.add(signature)

        # Process attachments
        for msg in messages:
            for att_info in msg['attachments']:
                attachment = {
                    'message_line': msg['line_number'],
                    'target_number': target_number,
                    'filename': att_info['filename'],
                    'directory_number': att_info['directory_number'],
                    'is_sticker': att_info['is_sticker'],
                    'relative_path': None,
                    'file_size': None
                }

                # Lookup file info from cache
                if att_info['filename'] in self.attachment_cache:
                    rel_path, file_size, dir_num = self.attachment_cache[att_info['filename']]
                    attachment['relative_path'] = rel_path
                    attachment['file_size'] = file_size

                attachments.append(attachment)

        return messages, attachments, tapbacks

    def _parse_tapback_line(self, line: str, target_number: str) -> Optional[Dict]:
        """Parse a tapback line"""
        pattern = r'(Loved|Liked|Disliked|Laughed at|Emphasized|Questioned) by (.+)'
        match = re.match(pattern, line.strip())
        if match:
            tapback_type = match.group(1)
            sender = match.group(2).strip()
            return {
                'target_number': target_number,
                'sender': sender,
                'tapback_type': tapback_type,
                'is_from_me': sender == "Me"
            }
        return None

    def _guess_mime_type(self, filename: str) -> str:
        """Guess MIME type from file extension"""
        ext = os.path.splitext(filename)[1].lower()
        mime_map = {
            '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
            '.gif': 'image/gif', '.bmp': 'image/bmp', '.webp': 'image/webp',
            '.heic': 'image/heic', '.heif': 'image/heif', '.tiff': 'image/tiff',
            '.mp4': 'video/mp4', '.mov': 'video/quicktime', '.avi': 'video/x-msvideo',
            '.mkv': 'video/x-matroska', '.webm': 'video/webm', '.m4v': 'video/mp4',
            '.mp3': 'audio/mpeg', '.wav': 'audio/wav', '.m4a': 'audio/mp4',
            '.aac': 'audio/aac', '.flac': 'audio/flac',
            '.pdf': 'application/pdf', '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.txt': 'text/plain', '.rtf': 'application/rtf'
        }
        return mime_map.get(ext, 'application/octet-stream')

    def import_conversation(self, conv_dir: Path, target_number: str) -> bool:
        """Import a single conversation"""
        # The actual txt file uses the target number as filename
        txt_file = conv_dir / f"{target_number.replace("+","p")}.txt"

        if not txt_file.exists():
            self.logger.warning(f"No conversation file found: {txt_file}")
            return False

        self.logger.info(f"Importing conversation: {target_number}")

        # Build attachment cache
        self.build_attachment_cache(conv_dir)

        # Parse conversation
        messages, attachments, tapbacks = self.parse_conversation_file(txt_file, target_number)

        if not messages:
            self.logger.warning(f"No messages found for {target_number}")
            return False

        cursor = self.connection.cursor()

        # Insert conversation record
        first_date = messages[0]['message_date'] if messages else None
        last_date = messages[-1]['message_date'] if messages else None

        cursor.execute('''
            INSERT OR REPLACE INTO conversations 
            (target_number, first_message_date, last_message_date)
            VALUES (?, ?, ?)
        ''', (target_number, first_date, last_date))

        # Batch insert messages
        message_data = []
        line_to_db_id = {}

        for msg in messages:
            message_data.append((
                msg['target_number'], msg['message_date'], msg['sender'],
                msg['message_text'], msg['is_from_me'], None,
                msg['message_type'], msg['indent_level'], msg['read_receipt_info'],
                msg.get('edited_text'), msg.get('edit_timestamp'), msg['is_unsent'],
                msg.get('expressive_type'), None,
                msg['line_number'], msg.get('is_duplicate', False)
            ))

        cursor.executemany('''
            INSERT INTO messages
            (target_number, message_date, sender, message_text, is_from_me,
             reply_to_message_id, message_type, indent_level, read_receipt_info,
             edited_text, edit_timestamp, is_unsent, expressive_type, special_data,
             line_number, is_duplicate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', message_data)

        # Get inserted message IDs and build mapping
        cursor.execute('''
            SELECT id, line_number FROM messages 
            WHERE target_number = ? 
            ORDER BY id
        ''', (target_number,))

        for db_id, line_num in cursor.fetchall():
            line_to_db_id[line_num] = db_id

        # Update reply relationships
        reply_updates = []
        for msg in messages:
            if msg['reply_to_message_id'] is not None and msg['line_number'] in line_to_db_id:
                parent_line = self._find_parent_message_line(messages, msg)
                if parent_line and parent_line in line_to_db_id:
                    reply_updates.append((
                        line_to_db_id[parent_line],
                        line_to_db_id[msg['line_number']]
                    ))

        if reply_updates:
            cursor.executemany('''
                UPDATE messages SET reply_to_message_id = ? WHERE id = ?
            ''', reply_updates)

        # Batch insert attachments
        attachment_data = []
        for att in attachments:
            if att['message_line'] in line_to_db_id:
                message_id = line_to_db_id[att['message_line']]
                attachment_data.append((
                    message_id, att['target_number'], att['filename'],
                    att['relative_path'], att['directory_number'], att['file_size'],
                    self._guess_mime_type(att['filename']), att['is_sticker']
                ))

        if attachment_data:
            cursor.executemany('''
                INSERT INTO attachments 
                (message_id, target_number, filename, relative_path, directory_number,
                 file_size, mime_type, is_sticker)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', attachment_data)

        # Batch insert tapbacks
        tapback_data = []
        for tapback in tapbacks:
            if tapback['target_message_line'] in line_to_db_id:
                message_id = line_to_db_id[tapback['target_message_line']]
                tapback_data.append((
                    message_id, tapback['target_number'], tapback['sender'],
                    tapback['tapback_type'], tapback['is_from_me']
                ))

        if tapback_data:
            cursor.executemany('''
                INSERT INTO tapbacks 
                (target_message_id, target_number, sender, tapback_type, is_from_me)
                VALUES (?, ?, ?, ?, ?)
            ''', tapback_data)

        # Update conversation statistics
        message_count = len([m for m in messages if not m.get('is_duplicate', False)])
        attachment_count = len(attachment_data)

        cursor.execute('''
            UPDATE conversations 
            SET message_count = ?, attachment_count = ?
            WHERE target_number = ?
        ''', (message_count, attachment_count, target_number))

        self.logger.info(f"  Imported {message_count} messages, {attachment_count} attachments")
        return True

    def _find_parent_message_line(self, messages: List[Dict], current_msg: Dict) -> Optional[int]:
        """Find the parent message line number for reply threading"""
        current_idx = None
        current_indent = current_msg['indent_level']

        # Find current message index
        for i, msg in enumerate(messages):
            if msg['line_number'] == current_msg['line_number']:
                current_idx = i
                break

        if current_idx is None or current_indent == 0:
            return None

        # Look backwards for a message with lower indent level
        for i in range(current_idx - 1, -1, -1):
            if messages[i]['indent_level'] < current_indent:
                return messages[i]['line_number']

        return None

    def build_database(self) -> bool:
        """Main function to build the complete database"""
        self.logger.info(f"Building iMessage database from: {self.export_dir}")

        try:
            # Create database schema
            self.create_database_schema()

            # Find all conversation directories
            conv_dirs = []
            for item in self.export_dir.iterdir():
                if item.is_dir() and item.name.startswith('p'):
                    try:
                        # Extract phone number from directory name (p1234567890 -> +1234567890)
                        phone_digits = item.name[1:]  # Remove 'p' prefix
                        if phone_digits.isdigit() and len(phone_digits) >= 10:
                            phone_number = '+' + phone_digits
                            conv_dirs.append((item, phone_number))
                    except:
                        continue

            if not conv_dirs:
                self.logger.error("No valid conversation directories found!")
                return False

            self.logger.info(f"Found {len(conv_dirs)} conversations to import")

            # Import each conversation
            success_count = 0
            for conv_dir, phone_number in conv_dirs:
                try:
                    if self.import_conversation(conv_dir, phone_number):
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to import {phone_number}: {e}")
                    continue

            if success_count == 0:
                self.logger.error("No conversations were successfully imported!")
                return False

            self.connection.commit()

            # Optimize database
            self.logger.info("Optimizing database...")
            self.connection.execute('ANALYZE')
            self.connection.execute('VACUUM')

            # Generate summary
            cursor = self.connection.cursor()

            cursor.execute('SELECT COUNT(*) FROM conversations')
            conv_count = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM messages WHERE is_duplicate = FALSE')
            msg_count = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM messages WHERE is_duplicate = TRUE')
            duplicate_count = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM attachments')
            att_count = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM tapbacks')
            tapback_count = cursor.fetchone()[0]

            self.logger.info(f"\n Database build completed successfully!")
            self.logger.info(f"   Conversations: {conv_count}")
            self.logger.info(f"   Messages: {msg_count:,} (unique)")
            self.logger.info(f"   Duplicates: {duplicate_count:,} (marked)")
            self.logger.info(f"   Attachments: {att_count:,}")
            self.logger.info(f"  ️ Tapbacks: {tapback_count:,}")
            self.logger.info(f"   Database: {self.db_path}")

            return True

        except Exception as e:
            self.logger.error(f"❌ Error building database: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if self.connection:
                self.connection.close()


def build_imessage_database(export_dir: str, db_path: str = "imessage.db") -> bool:
    """
    Build iMessage database from export directory
    
    Args:
        export_dir: Path to the export directory containing conversation subdirs
        db_path: Output database file path
    
    Returns:
        True if successful, False otherwise
    """
    builder = MessageDatabaseBuilder(export_dir, db_path)
    return builder.build_database()


if __name__ == "__main__":
    build_imessage_database("exported", "imessage.db")