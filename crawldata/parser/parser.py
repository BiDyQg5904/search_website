import subprocess

def get_content(url: str):
    node_file = 'parser/getContent.js'
    try:
        output = subprocess.check_output(['node', node_file, url], stderr=subprocess.STDOUT)
        return output.decode('utf-8')
    except subprocess.CalledProcessError as e:
        print()
        print("URL khong hop le")
        print()
        return None