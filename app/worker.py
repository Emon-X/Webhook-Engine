import logging
import time
from app.handlers.register import register_handlers
from app.services.event_handlers import dispatch_event
from app.services.webhook_worker import process_next_event

from app.db.session import SessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

def run_worker(handler, poll_interval = 2):
    
    logger.info("Starting webhook worker...")
    
    while True:
        
        try:
            with SessionLocal() as db:
                result = process_next_event(db, handler)
                
            if result is None:
                logger.debug("No eligible events found")
                time.sleep(poll_interval)
                
            else : 
                logger.info(
                    "Event finished successfully with status: %s",
                    result.id,
                    result.status
                )
        except KeyboardInterrupt:
            logger.info("Webhook worker stopped.")
            break
            
        except Exception:
            logger.exception("Unspected worder error")
            time.sleep(poll_interval)
            
if __name__ == "__main__":
    register_handlers()
    run_worker(dispatch_event)