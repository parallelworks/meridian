#!/bin/bash
# Runs as the container's main process (root). Sets up a one-node Slurm
# cluster and captures command outputs into /out.
# Usage: capture.sh acct|noacct
set -u
ACCT=$1
OUT=/out
mkdir -p $OUT
PLUG=/usr/lib/x86_64-linux-gnu/slurm-wlm
log() { echo "[$(date +%T)] $*" >&2; }
note() { echo "$*" >> $OUT/NOTES.txt; log "NOTE: $*"; }

VER=$(dpkg-query -W -f='${Version}' slurm-client); VER=${VER%%-*}
MAJ=${VER%%.*}; MIN=$(echo "$VER" | cut -d. -f2)
VNUM=$((MAJ * 100 + 10#$MIN))
echo "$VER" > $OUT/VERSION
dpkg -l | grep -E 'slurm|munge' > $OUT/dpkg.txt
log "slurm $VER ($VNUM) acct=$ACCT"

# ---- users, munge -------------------------------------------------------
useradd -u 1001 -m -s /bin/sh alice
if [ ! -s /etc/munge/munge.key ]; then
  (mungekey --create 2>/dev/null || dd if=/dev/urandom bs=1 count=1024 of=/etc/munge/munge.key 2>/dev/null)
  chown munge:munge /etc/munge/munge.key; chmod 400 /etc/munge/munge.key
fi
mkdir -p /run/munge /var/log/munge /var/lib/munge
chown munge:munge /run/munge /var/log/munge /var/lib/munge
runuser -u munge -- /usr/sbin/munged
for i in $(seq 20); do munge -n | unmunge >/dev/null 2>&1 && break; sleep 0.5; done
munge -n | unmunge >/dev/null || { note "munge not working"; exit 1; }

# ---- cgroup preparation ---------------------------------------------------
CGMODE=linuxproc
if [ -e $PLUG/cgroup_v2.so ] && [ -w /sys/fs/cgroup/cgroup.subtree_control ]; then
  CGMODE=cgroup
  mkdir -p /sys/fs/cgroup/init /sys/fs/cgroup/slurmd /sys/fs/cgroup/system.slice
  for pass in 1 2 3; do
    for p in $(cat /sys/fs/cgroup/cgroup.procs); do
      echo $p > /sys/fs/cgroup/init/cgroup.procs 2>/dev/null
    done
  done
  for c in $(cat /sys/fs/cgroup/cgroup.controllers); do
    echo "+$c" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || log "could not enable $c"
  done
  log "root subtree_control: $(cat /sys/fs/cgroup/cgroup.subtree_control)"
fi

mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /etc/slurm

write_conf() {
  local mode=$1
  {
    echo "ClusterName=fixture"
    echo "SlurmctldHost=n1"
    echo "SlurmUser=root"
    echo "SlurmdUser=root"
    echo "AuthType=auth/munge"
    echo "StateSaveLocation=/var/spool/slurmctld"
    echo "SlurmdSpoolDir=/var/spool/slurmd"
    echo "SlurmctldPidFile=/run/slurmctld.pid"
    echo "SlurmdPidFile=/run/slurmd.pid"
    echo "SlurmctldLogFile=/var/log/slurm/slurmctld.log"
    echo "SlurmdLogFile=/var/log/slurm/slurmd.log"
    echo "SchedulerType=sched/backfill"
    echo "SelectType=select/cons_tres"
    echo "SelectTypeParameters=CR_CPU"
    if [ "$mode" = cgroup ]; then
      echo "ProctrackType=proctrack/cgroup"
      echo "TaskPlugin=task/cgroup"
    else
      echo "ProctrackType=proctrack/linuxproc"
      if [ -e $PLUG/task_none.so ]; then echo "TaskPlugin=task/none"; fi
    fi
    echo "JobAcctGatherType=jobacct_gather/none"
    if [ "$ACCT" = acct ]; then
      echo "AccountingStorageType=accounting_storage/slurmdbd"
      echo "AccountingStorageHost=n1"
      echo "AccountingStorageEnforce=associations"
    else
      echo "AccountingStorageType=accounting_storage/none"
    fi
    echo "EnforcePartLimits=NO"
    echo "MinJobAge=60"
    echo "KillWait=5"
    echo "ReturnToService=2"
    echo "NodeName=n1 CPUs=2 RealMemory=1000 State=UNKNOWN"
    echo "PartitionName=debug Nodes=n1 Default=YES MaxTime=00:10:00 DefaultTime=00:05:00 State=UP"
    echo "PartitionName=down Nodes=n1 MaxTime=UNLIMITED State=DOWN"
    echo "PartitionName=hidden Nodes=n1 MaxTime=UNLIMITED Hidden=YES State=UP"
  } > /etc/slurm/slurm.conf
  rm -f /etc/slurm/cgroup.conf
  if [ "$mode" = cgroup ]; then
    {
      echo "CgroupPlugin=cgroup/v2"
      if [ $VNUM -ge 2302 ]; then echo "IgnoreSystemd=yes"; fi
    } > /etc/slurm/cgroup.conf
  fi
}

# ---- accounting -----------------------------------------------------------
if [ "$ACCT" = acct ]; then
  mkdir -p /run/mysqld; chown mysql:mysql /run/mysqld
  (mysqld_safe --user=mysql --skip-syslog > /var/log/slurm/mysqld_safe.out 2>&1 &)
  for i in $(seq 60); do mysqladmin ping >/dev/null 2>&1 && break; sleep 1; done
  mysql -e "CREATE DATABASE slurm_acct_db; CREATE USER 'slurm'@'localhost' IDENTIFIED BY 'slurmpw'; GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost'; FLUSH PRIVILEGES;" \
    || { note "mariadb setup failed"; exit 1; }
  cat > /etc/slurm/slurmdbd.conf <<'EOF'
AuthType=auth/munge
DbdHost=localhost
SlurmUser=root
LogFile=/var/log/slurm/slurmdbd.log
PidFile=/run/slurmdbd.pid
StorageType=accounting_storage/mysql
StorageHost=localhost
StorageUser=slurm
StoragePass=slurmpw
StorageLoc=slurm_acct_db
EOF
  chmod 600 /etc/slurm/slurmdbd.conf
  write_conf $CGMODE   # sacctmgr reads slurm.conf
  slurmdbd
  for i in $(seq 60); do sacctmgr -n list cluster >/dev/null 2>&1 && break; sleep 1; done
  sacctmgr -i add cluster fixture >&2
  sacctmgr -i add account fixacct Cluster=fixture Description=fixture Organization=fixture >&2
  sacctmgr -i add user alice Account=fixacct >&2
  sacctmgr -n -P list assoc format=cluster,account,user >&2
fi

start_slurm() {
  local mode=$1
  write_conf $mode
  rm -rf /var/spool/slurmctld/* /var/spool/slurmd/*
  slurmctld || return 1
  if [ "$mode" = cgroup ]; then
    sh -c 'echo $$ > /sys/fs/cgroup/slurmd/cgroup.procs && exec slurmd' || return 1
  else
    slurmd || return 1
  fi
  for i in $(seq 60); do
    st=$(sinfo -h -p debug -n n1 -o %t 2>/dev/null)
    [ "$st" = idle ] && break
    sleep 1
  done
  log "node state: $st"
  [ "$st" = idle ] || return 1
  # smoke test as root: a step must launch
  local t
  t=$(timeout 90 srun -p debug -N1 -n1 -t 1 cat /proc/self/cgroup 2>&1); local rc=$?
  log "smoke test rc=$rc: $t"
  [ $rc -eq 0 ] || return 1
  return 0
}

stop_slurm() {
  pkill -x slurmd; pkill -x slurmctld; pkill -x slurmstepd
  sleep 3
}

if ! start_slurm $CGMODE; then
  if [ "$CGMODE" = cgroup ]; then
    note "cgroup mode failed; slurmd log tail follows in run log; falling back to proctrack/linuxproc"
    tail -n 30 /var/log/slurm/slurmd.log >&2
    tail -n 30 /var/log/slurm/slurmctld.log >&2
    mkdir -p /var/log/slurm/cgroup-failure && cp /var/log/slurm/*.log /var/log/slurm/cgroup-failure/ 2>/dev/null
    stop_slurm
    : > /var/log/slurm/slurmd.log; : > /var/log/slurm/slurmctld.log
    CGMODE=linuxproc
    start_slurm linuxproc || { note "linuxproc mode failed too"; tail -n 50 /var/log/slurm/*.log >&2; exit 1; }
  else
    note "linuxproc mode failed"; tail -n 50 /var/log/slurm/*.log >&2; exit 1
  fi
fi
echo "$CGMODE" > $OUT/CGMODE
cp /etc/slurm/slurm.conf $OUT/slurm.conf
[ -e /etc/slurm/cgroup.conf ] && cp /etc/slurm/cgroup.conf $OUT/cgroup.conf
[ -e /etc/slurm/slurmdbd.conf ] && cp /etc/slurm/slurmdbd.conf $OUT/slurmdbd.conf

# ---- capture helpers -------------------------------------------------------
: > $OUT/commands.txt
cap() { # cap NN-name 'command' [stdin-text]
  local id=$1 cmd=$2
  if [ $# -ge 3 ]; then
    printf '%s' "$3" > $OUT/$id.stdin
    printf '%s  [as alice, stdin = %s.stdin]\n    %s\n' "$id" "$id" "$cmd" >> $OUT/commands.txt
    su -s /bin/sh alice -c "$cmd" < $OUT/$id.stdin > $OUT/$id.stdout 2> $OUT/$id.stderr
  else
    printf '%s  [as alice]\n    %s\n' "$id" "$cmd" >> $OUT/commands.txt
    su -s /bin/sh alice -c "$cmd" < /dev/null > $OUT/$id.stdout 2> $OUT/$id.stderr
  fi
  echo $? > $OUT/$id.exit
  log "cap $id exit=$(cat $OUT/$id.exit) out=$(head -c 200 $OUT/$id.stdout | tr '\n' ' ') err=$(head -c 300 $OUT/$id.stderr | tr '\n' ' ')"
}
state() { squeue -h -t all -j "$1" -o %T 2>/dev/null; }
reason() { squeue -h -t all -j "$1" -o %r 2>/dev/null; }
wait_state() { # id STATE timeout
  local i
  for i in $(seq "$3"); do
    [ "$(state "$1")" = "$2" ] && return 0
    sleep 1
  done
  note "job $1 did not reach $2 within $3s (state now: $(state "$1") reason $(reason "$1"))"
  return 1
}
sb() { # sb k 'extra opts' 'script' [partition] [time] [nodes] -> echoes command
  local k=$1 extra=$2 part=${4:-debug} time=${5:-2} nodes=${6:-1}
  mkdir -p /tmp/sub$k; chown alice:alice /tmp/sub$k
  echo "sbatch --parsable --no-requeue --export=ALL --job-name=meridian-a$k-1 --output=/tmp/sub$k/out --chdir=/tmp/sub$k --partition=$part --time=$time --nodes=$nodes$extra"
}
jid() { tr -d '\n' < $OUT/$1.stdout | cut -d';' -f1; }

SLEEP='#!/bin/sh
sleep 600
'

# ---- configuration -------------------------------------------------------
cap 01-sbatch-version 'sbatch --version'
cap 02-scontrol-show-config 'scontrol show config'
cap 03-scontrol-show-partition 'scontrol --all --oneliner show partition'
cap 03b-scontrol-show-partition-not-all 'scontrol --oneliner show partition'
cap 04-scontrol-show-federation 'scontrol show federation'

# ---- the job's environment ------------------------------------------------------
cap 05-sbatch-stdin "$(sb 1 '' '')" '#!/bin/sh
env | sort > /tmp/sub1/env; cat /proc/self/cgroup > /tmp/sub1/cgroup; sleep 600
'
A1=$(jid 05-sbatch-stdin)
wait_state "$A1" RUNNING 60
for i in $(seq 30); do [ -s /tmp/sub1/cgroup ] && break; sleep 1; done
sleep 1
cp /tmp/sub1/env $OUT/05-job-env.txt 2>/dev/null || note "no /tmp/sub1/env"
cp /tmp/sub1/cgroup $OUT/05-job-cgroup.txt 2>/dev/null || note "no /tmp/sub1/cgroup"
cap 11a-squeue-start-end-running "SLURM_TIME_FORMAT=%s squeue --noheader --jobs=$A1 --format='%S|%e'"
cap 11c-posixly-correct-short "POSIXLY_CORRECT=1 squeue -h -t all -j $A1 -o %i"
cap 11d-posixly-correct-long "POSIXLY_CORRECT=1 squeue --noheader --states=all --jobs=$A1 --format=%i"

# ---- jobs in each state ---------------------------------------------------------
cap 06a-sbatch-held "$(sb 2 ' --hold')" "$SLEEP"; A2=$(jid 06a-sbatch-held)
cap 06b-sbatch-over-partition-time "$(sb 3 '' '' debug 20)" "$SLEEP"; A3=$(jid 06b-sbatch-over-partition-time)
cap 06c-sbatch-partition-down "$(sb 4 '' '' down)" "$SLEEP"; A4=$(jid 06c-sbatch-partition-down)
cap 06d-sbatch-exit0 "$(sb 5 '')" '#!/bin/sh
exit 0
'; A5=$(jid 06d-sbatch-exit0)
wait_state "$A5" COMPLETED 60
cap 06e-sbatch-exit3 "$(sb 6 '')" '#!/bin/sh
exit 3
'; A6=$(jid 06e-sbatch-exit3)
wait_state "$A6" FAILED 60
cap 06f-sbatch-to-cancel "$(sb 7 '')" "$SLEEP"; A7=$(jid 06f-sbatch-to-cancel)
wait_state "$A7" RUNNING 60
cap 06g-scancel-running "scancel $A7"
wait_state "$A7" CANCELLED 60
cap 06h-sbatch-timeout "$(sb 8 '' '' debug 1)" '#!/bin/sh
sleep 300
'; A8=$(jid 06h-sbatch-timeout)
wait_state "$A8" RUNNING 60
cap 06i-sbatch-resources "$(sb 9 ' --cpus-per-task=2')" "$SLEEP"; A9=$(jid 06i-sbatch-resources)
cap 06j-sbatch-nodes-ntasks "$(sb 12 ' --hold --ntasks=1 --cpus-per-task=1' '' debug 2 2)" "$SLEEP"; A12=$(jid 06j-sbatch-nodes-ntasks)
cap 06k-sbatch-nodes-ntasks-per-node "$(sb 13 ' --hold --ntasks-per-node=1 --cpus-per-task=1' '' debug 2 2)" "$SLEEP"; A13=$(jid 06k-sbatch-nodes-ntasks-per-node)
[ -n "$A12" ] && cap 06l-scontrol-show-job-nodes-ntasks "scontrol --oneliner show job $A12"
[ -n "$A13" ] && cap 06m-scontrol-show-job-nodes-ntasks-per-node "scontrol --oneliner show job $A13"
cap 06n-sbatch-hidden "$(sb 11 ' --hold' '' hidden)" "$SLEEP"; A11=$(jid 06n-sbatch-hidden)
sleep 3
cap 11b-squeue-start-end-pending "SLURM_TIME_FORMAT=%s squeue --noheader --jobs=$A2 --format='%S|%e'"
ALL="$A1,$A2,$A3,$A4,$A5,$A6,$A7,$A8,$A9"
cap 07a0-squeue-all-before-timeout "squeue --noheader --states=all --jobs=$ALL --format='%i|%T|%r'"
wait_state "$A8" TIMEOUT 240
echo "a1=$A1 sleep600 running; a2=$A2 held; a3=$A3 over-partition-time; a4=$A4 partition-down; a5=$A5 exit0; a6=$A6 exit3; a7=$A7 cancelled; a8=$A8 timeout; a9=$A9 cpus-per-task=2; a11=$A11 hidden, held; a12=$A12 nodes=2 ntasks=1, held; a13=$A13 nodes=2 ntasks-per-node=1, held" > $OUT/JOBS.txt

# ---- squeue ---------------------------------------------------------------------
cap 07a-squeue-all "squeue --noheader --states=all --jobs=$ALL --format='%i|%T|%r'"
cap 08-squeue-unknown "squeue --noheader --states=all --jobs=999999 --format='%i|%T|%r'"
cap 09-squeue-real-and-unknown "squeue --noheader --states=all --jobs=$A1,999999 --format='%i|%T|%r'"
cap 10-squeue-user "squeue --noheader --all --user=1001 --states=all --format='%i|%j'"
cap 10b-squeue-user-not-all "squeue --noheader --user=1001 --states=all --format='%i|%j'"

# ---- sacct ----------------------------------------------------------------------
cap 12-sacct-jobs "sacct --allocations --noheader --parsable2 --jobs=$ALL --format=JobIDRaw,State"
if [ "$ACCT" = acct ]; then
  cap 13-sacct-user-names "sacct --allocations --noheader --parsable2 --user=1001 --starttime=now-30days --name=meridian-a1-1,meridian-a5-1 --format=JobIDRaw,State,JobName"
fi

# ---- scancel ----------------------------------------------------------------------
cap 14a-scancel-completed "scancel $A5"
cap 14b-scancel-unknown "scancel 999999"

# ---- sbatch refusals --------------------------------------------------------------
mkdir -p /tmp/subr; chown alice:alice /tmp/subr
R='--output=/tmp/subr/out --chdir=/tmp/subr'
cap 15a-sbatch-partition-nosuch "sbatch --parsable --no-requeue --export=ALL --job-name=meridian-r1-1 $R --partition=nosuch --time=2 --nodes=1" "$SLEEP"
cap 15b-sbatch-time-bogus "sbatch --parsable --no-requeue --export=ALL --job-name=meridian-r2-1 $R --partition=debug --time=bogus --nodes=1" "$SLEEP"
cap 15c-sbatch-nodes-5 "sbatch --parsable --no-requeue --export=ALL --job-name=meridian-r3-1 $R --partition=debug --time=2 --nodes=5" "$SLEEP"
cap 15d-sbatch-mem-too-big "sbatch --parsable --no-requeue --export=ALL --job-name=meridian-r4-1 $R --partition=debug --time=2 --nodes=1 --mem=999999999" "$SLEEP"
if [ "$ACCT" = acct ]; then
  cap 15e-sbatch-account-nosuch "sbatch --parsable --no-requeue --export=ALL --job-name=meridian-r5-1 $R --partition=debug --time=2 --nodes=1 --account=nosuch" "$SLEEP"
fi

# ---- a drained node -----------------------------------------------------------------
scontrol update NodeName=n1 State=DRAIN Reason=capture
cap 16a-sbatch-drained "$(sb 14 '')" "$SLEEP"; A14=$(jid 16a-sbatch-drained)
sleep 5
cap 16b-squeue-drained "squeue --noheader --states=all --jobs=$A14 --format='%i|%T|%r'"
scontrol update NodeName=n1 State=RESUME
scancel $A14

# ---- after MinJobAge ---------------------------------------------------------------
log "waiting for finished jobs to be purged"
for i in $(seq 900); do
  left=$(squeue -h -t all -j $A5,$A6,$A7,$A8,$A1 -o %i 2>/dev/null | wc -l)
  [ "$left" -eq 0 ] && break
  sleep 1
done
[ "$left" -eq 0 ] || note "finished jobs not all purged after 900s"
cap 07b-squeue-all-after-minjobage "squeue --noheader --states=all --jobs=$ALL --format='%i|%T|%r'"
cap 09b-squeue-purged-and-unknown "squeue --noheader --states=all --jobs=$A5,999999 --format='%i|%T|%r'"
cap 09c-squeue-purged-alone "squeue --noheader --states=all --jobs=$A5 --format='%i|%T|%r'"
cap 14c-scancel-purged "scancel $A5"
cap 12b-sacct-jobs-after-minjobage "sacct --allocations --noheader --parsable2 --jobs=$ALL --format=JobIDRaw,State"

squeue -t all -o '%i|%T|%r|%j' > /var/log/slurm/final-squeue.txt 2>&1
log "done"
