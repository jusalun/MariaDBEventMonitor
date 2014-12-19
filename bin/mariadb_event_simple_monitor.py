#coding: UTF-8
__author__ = 'yangchenxing'

import datetime
import json
import time

import gevent
import gevent.event
import gevent.monkey
import gevent.subprocess

import jesgoo.application
import jesgoo.database.mysql_cursor
import jesgoo.supervisorutil.log as log

gevent.monkey.patch_all()


class MariaDBEventSimpleHandler(object):
    def __init__(self, command, min_delay, max_delay, delay_step, command_timeout, **_):
        self._command = command
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._delay_step = delay_step
        self._command_timeout = command_timeout
        self._event = gevent.event.Event()

    def __call__(self):
        self._event.set()

    def run(self):
        while 1:
            self._event.wait()
            deadline = time.time() + self._max_delay
            if self._event.wait(self._min_delay):
                while self._event.wait(min(self._delay_step, deadline - time.time())):
                    pass
            command_start_time = time.time()
            command = gevent.subprocess.Popen(self._command, shell=True)
            status = command.wait(self._command_timeout)
            command_end_time = time.time()
            if status is None:
                command.kill()
                log.error('指令执行超时, command="%s", timeout=%d', self._command, self._command_timeout)
            elif status != 0:
                log.error('指令执行失败, command="%s", status=%d, time=%f',
                          self._command, status, command_end_time - command_start_time)
            else:
                log.info('指令执行成功, command="%s, time=%f', self._command, command_end_time - command_start_time)


class MariaDBEventSimpleMonitor(jesgoo.application.Application):
    def __init__(self, *args, **kwargs):
        super(MariaDBEventSimpleMonitor, self).__init__('..', *args, **kwargs)
        self._progress = None
        self._handlers = {}

    def parse_args(self):
        parser = self._create_default_argument_parser('MariaDB更新事件监控')
        return parser.parse_args()

    def main(self, args):
        with open(self.config.simple_monitor.progress_file, 'rb') as f:
            self._progress = json.load(f)
        self._handlers = {handler.name: MariaDBEventSimpleHandler(**handler)
                          for handler in self.config.simple_monitor.handlers}
        jesgoo.database.mysql_cursor.MySQLCursor.create_connection_pools(**self.config.mariadb.as_config_dict)
        for table in self.config.tables:
            gevent.spawn(self.monitor_table, **table)

    def monitor_table(self, database, name, handlers):
        handlers = map(lambda x: self._handlers[x], handlers)
        progress_key = database + '.' + name
        progress = datetime.datetime.strptime(self._progress.get(progress_key, '1970-01-01:00:00:00'),
                                              '%Y-%m-%d:%H:%M:%S')
        while 1:
            with jesgoo.database.mysql_cursor.MySQLCursor(database) as cursor:
                cursor.execute('SELECT MAX(`modified_time`) AS `last_modified_time` FROM `%s`' % (name,))
                last_modified_time = cursor.fetchone().last_modified_time
                if last_modified_time > progress:
                    for handler in handlers:
                        handler()
                progress = last_modified_time
                self.save_progress(progress_key, progress)
            gevent.sleep(self.config.simple_monitor.interval)

    def save_progress(self, progress_key, last_modified_time):
        self._progress[progress_key] = last_modified_time.strftime('%Y-%m-%d:%H:%M:%S')
        with open(self.config.simple_monitor.progress_file, 'wb') as f:
            f.write(json.dumps(self._progress, indent=4))


if __name__ == '__main__':
    application = MariaDBEventSimpleMonitor()
    application.run()