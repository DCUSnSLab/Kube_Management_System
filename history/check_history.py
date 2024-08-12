import os
import sys

class checkHistory():
    def __init__(self, isDegug=False):
        self.DegugMode = isDegug
        self.bash_history = self.load_file("~/.bash_history")
        self.file = self.load_file("~/.profiling/.rec")
        self.previousFile = self.load_file("~/.profiling/.rec_latest")

    def save_history(self):
        # rec이 이미 있을 경우 기존 파일을 rec_latest로 이름 변경하고 저장
        if self.check_file_exists(self.file):
            self.change_previousfile(self.file)
        try: #bash_history의 최근 30개를 저장
            with open(self.bash_history, "r") as h:
                lines = h.readlines()
            recent_lines = lines[-30:]
            with open(self.file, "w") as l:
                l.writelines(recent_lines)
            print("History saved successfully.")
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    def load_file(self, file):
        return os.path.expanduser(file)

    def check_file_exists(self, file):
        return os.path.exists(file)

    def change_previousfile(self, file):
        try:
            return os.rename(file, self.previousFile)
        except FileExistsError:
            #이미 존재하여 오류날 경우 삭제하고 이름 변경
            os.remove(self.previousFile)
            return os.rename(file, self.previousFile)
        except Exception as e:
            print(f"Error: {e}")

    def compare(self, file1_path, file2_path):
        with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2:
            file1 = f1.read()
            file2 = f2.read()
        return file1 == file2

    def start(self):
        if self.DegugMode == True:
            print("Degug mode on!!")
            if input("diff or same: ") == "diff":
                self.file = self.load_file("testfile/diff_rec")
                self.previousFile = self.load_file("testfile/diff_rec_latest")
            else:
                self.file = self.load_file("testfile/same_rec")
                self.previousFile = self.load_file("testfile/same_rec_latest")
        else:
            self.save_history()
        if not self.check_file_exists(self.previousFile):
            print("No exist Latest file")
            sys.exit(0)

        print("compare...")
        if self.compare(self.file, self.previousFile):
            print("Same as previous record.")
        else:
            print("There are records added.")

if __name__ == "__main__":
    ch = checkHistory(isDegug=False)
    ch.start()
