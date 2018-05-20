#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import flask
from flask import make_response, render_template, request, redirect, url_for
from kafka_util import KafkaHelper
import log_util

app = flask.Flask(__name__)
log = log_util.get_logger(tag='kafka_console')

def get_kafka_topic_view(consumer, topic):
    log.info("consumer: %s, topic: %s", consumer, topic)
    with KafkaHelper() as h:
        lags = []
        lag = 0
        log.info('offset, logsize, lag')
        partitions = h.get_partitions(topic)
        for p in partitions:
            offset = h.consumer_offset(consumer, topic, p)
            logsize = h.current_offset(topic, p)
            lag = logsize - offset
            log.info("%s, %s, %s, %s", p, offset, logsize, lag)
            lags.append(lag)
            
        log.info("total lags: %s", sum(lags))

@app.route("/reset/<consumer_group>/<topic>")
def skip_all_msg(consumer_group, topic):
    with KafkaHelper() as h:
        consumer = h.get_consumer(consumer_group, topic)
        consumer.seek(0, 2) # set offset - lags= 0, will skip all msg

    return redirect("/kafka/{}/{}".format(consumer_group, topic))

@app.route("/kafka/<consumer>/<topic>")
def view_kafka(consumer, topic):
    consumer = str(consumer)
    topic = str(topic)
    rows = []
    with KafkaHelper() as h:
        partitions = h.get_partitions(topic)
        for p in partitions:
            offset = h.consumer_offset(consumer, topic, p)
            logsize = h.current_offset(topic, p)
            lag = logsize - offset
            rows.append((p, offset, logsize, lag))
    
    total_offset = sum([i[1] for i in rows])
    total_logsize = sum([i[2] for i in rows])
    total_lag = sum([i[3] for i in rows])
    rows.append(('total', total_offset, total_logsize, total_lag))
    rs={'topic_rows':rows, 'consumer_group':consumer, 'topic': topic, 
        'total_lag':total_lag}
    #return "consumer group: {}, topic: {}<br>".format(consumer, topic) + get_html_table(rows, thead)
    req_json = request.args.get('json')
    if req_json:
        return json.dumps(rs)
    return render_template('console.html', rs=rs)

    
if __name__ == '__main__':
    import os, time
    mt = os.stat(sys.argv[0]).st_mtime
    log.info("Code update time: %s", time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(mt)))
     
    app.run(debug=True, host='0.0.0.0')
