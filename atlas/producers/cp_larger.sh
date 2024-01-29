#!/bin/bash
(( ($(stat -c"%s-" ihr-global.conf_new ihr-global.conf) 0)  > -4000 )) && cp ihr-global.conf_new ihr-global.conf
