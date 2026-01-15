from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Current Directory: {current_dir}")

for filepath in current_dir.iterdir():
    if filepath.name == current_file:
        continue

    print(f"Found file: {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8')
        print(f"Content of {filepath.name}:\n{content}\n")
        