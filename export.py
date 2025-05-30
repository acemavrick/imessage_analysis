import shutil
import os
import re

def get_all_exported_numbers(reinstate_plus = True):
    """
    Get all exported phone numbers from the exported directory.

    Returns:
        A list of phone numbers that have been exported.
    """
    exported_dir = os.path.join(os.getcwd(), "exported")
    if not os.path.exists(exported_dir):
        print("No exported directory found.")
        return []

    # List all directories in the exported directory
    exported_numbers = [d.replace("p", "+") for d in os.listdir(exported_dir) if os.path.isdir(os.path.join(exported_dir, d))]
    return exported_numbers

def clean_exported_dir():
    exported_dir = os.path.join(os.getcwd(), "exported")
    if not os.path.exists(exported_dir):
        print("No exported directory found.")
        return

    # rename all files
    for root, dirs, files in os.walk(exported_dir):
        for file_name in files:
            if '+' in file_name:
                old_path = os.path.join(root, file_name)
                new_file_name = file_name.replace('+', 'p')
                new_path = os.path.join(root, new_file_name)
                if os.path.exists(new_path):
                    os.remove(new_path)  # remove existing file
                os.rename(old_path, new_path)

    # rename all directories (bottom-up)
    for root, dirs, files in os.walk(exported_dir, topdown=False):
        for dir_name in dirs:
            if '+' in dir_name:
                old_path = os.path.join(root, dir_name)
                new_dir_name = dir_name.replace('+', 'p')
                new_path = os.path.join(root, new_dir_name)
                if os.path.exists(new_path):
                    shutil.rmtree(new_path)  # remove existing directory and contents
                os.rename(old_path, new_path)

    print("Cleaned up exported directory. All '+' signs replaced with 'p'.")

def get_attachments_to_keep(export_dir, filename):
    '''
    scan filename.txt and return set of attachment filenames to keep

    Args:
        export_dir:
            directory where the export is stored
        filename:
            name of the file to scan, without extension

    Returns:
        set of attachment filenames to keep
    '''
    attachments = set()
    txt_path = os.path.join(export_dir, f"{filename}.txt")
    with open(txt_path, 'r', encoding = 'utf-8') as f:
        content = f.read()

    # Look for attachment patterns in the text
    # Based on Rust tool output format
    patterns = [
        # Full paths: /path/to/file.jpg
        r'([/\\][\w\s/\\.-]+\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|webm|m4v|mp3|wav|m4a|aac|flac|pdf|doc|docx|txt|rtf|heic))',
        # Relative/just filename: filename.jpg
        r'([\w\s.-]+\.(jpg|jpeg|png|gif|bmp|tiff|webp|mp4|mov|avi|mkv|webm|m4v|mp3|wav|m4a|aac|flac|pdf|doc|docx|txt|rtf|heic))(?=\s|$)',
        # Sticker format: "Sticker from Me: /path/file.heic"
        r'Sticker from \w+: (.+?)(?:\s|$|\()',
    ]

    for pattern in patterns:
        matches = re.finditer(pattern, content, re.IGNORECASE)
        for match in matches:
            full_path = match.group(1).strip()
            # Extract just the filename
            filename_only = os.path.basename(full_path)
            if filename_only and len(filename_only) > 3:  # Sanity check
                attachments.add(filename_only)
    return attachments

def export(target_num, force = False, include_all_groups = False, do_not_clean = False):
    """
    Export iMessage data for a given target number.


    Args:
        target_num: The phone number to export iMessage data for, in the format "+1234567890".
        force: If True, forces the export even if the data already exists.
        include_all_groups: If True, prevents post-export trimming of all groupchats from the export.
            This would mean that the export will include all conversations, including group chats, that include the target number.

    Returns:
        True if the export was successful, False otherwise.
    """
    path = os.path.join(os.getcwd(), "exported", target_num)

    # export
    if force and os.path.exists(path):
        # remove existing directory if it exists
        cmd = f'rm -rf "{path}"'
        if os.system(cmd) != 0:
            print(f"Failed to remove existing export directory for {target_num}. (forced)")
            return False

    command = f"imessage-exporter -f txt -c clone -t {target_num} -o {path}"
    if os.system(command) != 0:
        print(f"Failed to export iMessage data for {target_num}.")
        return False
    print(f"Successfully exported iMessage data for {target_num} to {path}.")

    # trim if necessary
    if not include_all_groups:
        # remove txt files that are not dms
        cmd = f'find {path} -maxdepth 1 -type f -name "*.txt" ! -name "{target_num}.txt" -exec rm {{}} +'
        if os.system(cmd) != 0:
            print(f"Failed to trim iMessage data for {target_num}.")
            return False
        print(f"Successfully trimmed iMessage conversations for {target_num}.")

        # remove attachments that are not in the conversation
        tokeep = get_attachments_to_keep(path, target_num)
        if tokeep:
            name_exclusions = " ".join([f"! -name '{name}'" for name in tokeep])
            cmd = f'find {path} -type f {name_exclusions} ! -name "{target_num}.txt" -exec rm {{}} +'
            if os.system(cmd) != 0:
                print(f"Failed to remove attachments for {target_num}.")
                return False
            print(f"removed attachments for {target_num} that are not in the conversation.")

        # clean up empty directories
        cmd = f'find {path} -type d -empty -delete'
        if os.system(cmd) != 0:
            print(f"Failed to clean up empty directories for {target_num}.")
            return False
        print(f"cleaned up empty directories for {target_num}.")
    print(f"Successfully exported iMessage data for {target_num} to {path}.")

    if not do_not_clean:
        clean_exported_dir()

    return True

if __name__ == "__main__":
    nums = get_all_exported_numbers()
    print("Exporting the following numbers:", nums)
    for num in nums:
        export(num, force=True, do_not_clean=True)
    print("All exports completed.")
    clean_exported_dir()