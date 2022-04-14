import signal
import time

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True

class NormalKiller:
    kill_now = False

if __name__ == '__main__':
    killer = NormalKiller()
    while not killer.kill_now:
        print("hi")
        time.sleep(10)
        print("doing something in a loop ...")
        break

    print("End of the program. I was killed gracefully :)")