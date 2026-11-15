#!/bin/bash
# Runs as a container's main process (root): a one-node cluster whose
# controller also takes JWTs, where the release has auth/jwt, and what
# sbatch does with SLURM_JWT, and without munge, into /out. A job prints
# its environment with a token's value as <token>, so no token is kept.
set -u
OUT=/out
mkdir -p $OUT
PLUG=/usr/lib/x86_64-linux-gnu/slurm-wlm
VER=$(dpkg-query -W -f='${Version}' slurm-client); echo "${VER%%-*}" > $OUT/VERSION
dpkg -l | grep -E 'slurm|munge' > $OUT/dpkg.txt
useradd -u 1001 -m -s /bin/sh alice
echo 'export FROM_LOGIN_PROFILE=yes' >> /home/alice/.profile
(mungekey --create 2>/dev/null || dd if=/dev/urandom bs=1 count=1024 of=/etc/munge/munge.key 2>/dev/null)
chown munge:munge /etc/munge/munge.key; chmod 400 /etc/munge/munge.key
mkdir -p /run/munge /var/log/munge /var/lib/munge /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /etc/slurm
chown munge:munge /run/munge /var/log/munge /var/lib/munge
runuser -u munge -- /usr/sbin/munged
sleep 1
JWT=""
if [ -e $PLUG/auth_jwt.so ]; then
  dd if=/dev/urandom bs=32 count=1 2>/dev/null > /var/spool/slurmctld/jwt.key; chmod 600 /var/spool/slurmctld/jwt.key
  JWT="AuthAltTypes=auth/jwt
AuthAltParameters=jwt_key=/var/spool/slurmctld/jwt.key"
fi
cat > /etc/slurm/slurm.conf <<CONF
ClusterName=fixture
SlurmctldHost=n1
SlurmUser=root
SlurmdUser=root
AuthType=auth/munge
$JWT
StateSaveLocation=/var/spool/slurmctld
SlurmdSpoolDir=/var/spool/slurmd
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
SchedulerType=sched/backfill
SelectType=select/cons_tres
SelectTypeParameters=CR_CPU
ProctrackType=proctrack/linuxproc
TaskPlugin=task/none
JobAcctGatherType=jobacct_gather/none
AccountingStorageType=accounting_storage/none
NodeName=n1 CPUs=2 RealMemory=1000 State=UNKNOWN
PartitionName=debug Nodes=n1 Default=YES MaxTime=00:10:00 State=UP
CONF
[ -e $PLUG/task_none.so ] || sed -i '/TaskPlugin/d' /etc/slurm/slurm.conf
if [ -e $PLUG/cgroup_v2.so ]; then
  mkdir -p /sys/fs/cgroup/init /sys/fs/cgroup/slurmd /sys/fs/cgroup/system.slice
  for pass in 1 2 3; do for p in $(cat /sys/fs/cgroup/cgroup.procs); do echo $p > /sys/fs/cgroup/init/cgroup.procs 2>/dev/null; done; done
  for c in $(cat /sys/fs/cgroup/cgroup.controllers); do echo "+$c" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null; done
  sed -i 's#ProctrackType=.*#ProctrackType=proctrack/cgroup#; /TaskPlugin/d' /etc/slurm/slurm.conf; echo TaskPlugin=task/cgroup >> /etc/slurm/slurm.conf
  printf 'CgroupPlugin=cgroup/v2\nIgnoreSystemd=yes\n' > /etc/slurm/cgroup.conf
  slurmctld; sh -c 'echo $$ > /sys/fs/cgroup/slurmd/cgroup.procs && exec slurmd'
else
  slurmctld; slurmd
fi
for i in $(seq 60); do [ "$(sinfo -h -o %t 2>/dev/null)" = idle ] && break; sleep 1; done
cp /etc/slurm/slurm.conf $OUT/slurm.conf
: > $OUT/commands.txt
cap() { # cap NN-name 'command' [stdin]: as alice; a token is written $TOK
  local shown=$2
  [ -n "${TOK:-}" ] && shown=${shown//$TOK/\$TOK}
  printf '%s\n    %s\n' "$1" "$shown" >> $OUT/commands.txt
  printf '%s' "${3:-}" > $OUT/$1.stdin
  su -s /bin/sh alice -c "cd /tmp && $2" < $OUT/$1.stdin > $OUT/$1.stdout 2> $OUT/$1.stderr
  echo $? > $OUT/$1.exit
  [ -s $OUT/$1.stdin ] || rm -f $OUT/$1.stdin
  for e in stdout stderr; do [ -s $OUT/$1.$e ] || rm -f $OUT/$1.$e; done
}
SCRIPT='#!/bin/sh
env | sed -E "s/^SLURM_JWT=.+/SLURM_JWT=<token>/" | sort > /tmp/env.$SLURM_JOB_ID
'
job() { # job NN-name 'environment' export: submits SCRIPT, and keeps the job's environment
  cap "$1" "env $2 sbatch --parsable --export=$3 --output=/tmp/out.%j" "$SCRIPT"
  local id; id=$(cat $OUT/$1.stdout 2>/dev/null | cut -d';' -f1)
  for i in $(seq 40); do [ -n "$id" ] && [ -s /tmp/env.$id ] && break; sleep 1; done
  [ -n "$id" ] && cp /tmp/env.$id $OUT/$1-job-env.txt 2>/dev/null
}
job 01-sbatch-export-all "SLURM_FOO=x" ALL
job 02-sbatch-export-all-slurm-foo-empty "SLURM_FOO=x" ALL,SLURM_FOO=
if [ -n "$JWT" ]; then
  TOK=$(scontrol token username=alice lifespan=600 | sed 's/^SLURM_JWT=//')
  job 03-sbatch-jwt-export-all "SLURM_JWT=$TOK" ALL
  job 04-sbatch-jwt-export-all-jwt-empty "SLURM_JWT=$TOK" ALL,SLURM_JWT=
  cap 05-squeue-jwt "env SLURM_JWT=$TOK squeue --noheader --states=all --format=%i"
fi
# Without munge, as on a login node that has none, or while munged restarts.
pkill -x munged; sleep 1
cap 06-sbatch-no-munge "sbatch --parsable --export=ALL --output=/tmp/out.%j" "$SCRIPT"
cap 07-squeue-no-munge "squeue --noheader --states=all --format=%i"
if [ -n "$JWT" ]; then
  cap 08-sbatch-no-munge-jwt "env SLURM_JWT=$TOK sbatch --parsable --export=ALL --output=/tmp/out.%j" "$SCRIPT"
fi
echo done
