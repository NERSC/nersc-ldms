#!/bin/bash
tmux new-session -d -s ldms_sessions

cmd="/ldms_sampler.bash"
tmux split-window -t ldms_sessions
tmux send-keys -t ldms_sessions "$cmd" C-m
tmux select-pane -t ldms_sessions.1 -T "$cmd"

cmd="/ldms_agg.bash"
tmux split-window -t ldms_sessions
tmux send-keys -t ldms_sessions "$cmd" C-m
tmux select-pane -t ldms_sessions.2 -T "$cmd"

#cmd="/bin/bash"
#tmux split-window -t ldms_sessions
#tmux select-pane -t ldms_sessions.2 -T "$cmd"

tmux select-layout even-vertical
tmux set -g pane-border-status top
tmux set -g pane-border-format " [ ###P #T ] "
tmux attach-session -t ldms_sessions.0
#tmux attach -t ldms_sessions
# When they exit
tmux list-sessions
tmux kill-session -t ldms_sessions

