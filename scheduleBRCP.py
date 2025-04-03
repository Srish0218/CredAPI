import time
import schedule
import subprocess
import sys


# Function to ensure required packages are installed
def install_requirements():
    try:
        import zulip  # Check if 'zulip' is installed
    except ModuleNotFoundError:
        print("Installing missing dependencies...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        print("Dependencies installed successfully.")


# Function to run the Python script
def run_script():
    print("\n=== Running Script at:", time.strftime('%Y-%m-%d %H:%M:%S'), "===")

    try:
        subprocess.run([sys.executable, "getBrcpOutput.py"], check=True)  # Runs getBrcpOutput.py
        print("Execution Successful")
    except subprocess.CalledProcessError as e:
        print(f"Error running script: {e}")


# Install requirements once before scheduling
install_requirements()

# Run the script once before scheduling starts
run_script()

# Schedule the script to run every 10 minutes
schedule.every(60).seconds.do(run_script)

# Run scheduler indefinitely
if __name__ == "__main__":
    print("Scheduler is running... (Executing getBrcpOutput.py every 10 minutes)")

    while True:
        schedule.run_pending()
        time.sleep(30)  # Check every 30 seconds
