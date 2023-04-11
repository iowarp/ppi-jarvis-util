import psutil

class Kill:
    def __init__(self, cmd):
        # Iterate over all processes
        for proc in psutil.process_iter():
            try:
                # Get the process name
                process_name = proc.name()

                # Check if the process name contains the substring
                if cmd in process_name:
                    # Kill the process
                    proc.kill()
                    print(f"Killed process: {process_name}")
            except (psutil.NoSuchProcess,
                    psutil.AccessDenied,
                    psutil.ZombieProcess):
                print(f"Failed to kill: {process_name}")
                pass
