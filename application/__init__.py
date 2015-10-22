from application.server import run
import threading


process_thread = threading.Thread(name='banks_processor', target=run)
process_thread.daemon = True
process_thread.start()
