import os
import gevent
import gevent.queue
import logging


from gevent import monkey
monkey.patch_all()
logger = logging.getLogger(
    __name__
)

_q = gevent.queue.JoinableQueue()


def walk(base):
    try:
        objects = os.listdir(base)
    except Exception as error:
        logging.error(error)

        return
    full_path = None
    for obj in objects:

        full_path = os.path.join(
            base, obj
        )

        if os.path.isdir(full_path):
            _q.put(
                full_path
            )

        else:
            try:
                # pass
                pass
                #os.stat(full_path)
            except OSError:
                pass


def worker():
    while True:
        try:
            walk(_q.get())

        finally:
            _q.task_done()



if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO
    )

    for i in range(50):
        gevent.spawn(worker)

    walk('/ice/shows/erhetest003/')

    _q.join()

