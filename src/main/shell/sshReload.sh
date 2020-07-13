#!/usr/bin/expect

set timeout  600

spawn /usr/bin/ssh hadoop@10.11.9.6

expect "*hadoop*"
send "sudo su\r"

expect {
        "*hadoop*" {send "hadoop\r"}
}

expect "*]#" {send "/opt/application/analyze/analyze_project/reload.sh\r"}

expect "*]#" {send "exit\r"}
