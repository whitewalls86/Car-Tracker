import os

VALID_UA_LOG = "valid_user_agents.txt"
SUCCESS_UA_LOG = "successful_user_agents.log"
FAILED_UA_LOG = "failed_user_agents.log"


def read_user_agent_set(file_path):
    if not os.path.exists(file_path):
        return set()
    with open(file_path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def write_user_agent_set(file_path, ua_set):
    with open(file_path, "w", encoding="utf-8") as f:
        for ua in sorted(ua_set):
            f.write(ua + "\n")


def update_valid_user_agents(success_set=None, failed_set=None):
    if success_set is None:
        success_set = read_user_agent_set(SUCCESS_UA_LOG)
    if failed_set is None:
        failed_set = read_user_agent_set(FAILED_UA_LOG)

    valid_set = success_set - failed_set
    if valid_set:
        write_user_agent_set(VALID_UA_LOG, valid_set)
    else:
        print("[WARNING] Skipping write to valid_user_agents.txt (would be empty)")


def log_user_agent(ua_string, success=True):
    ua_string = ua_string.strip()
    success_set = read_user_agent_set(SUCCESS_UA_LOG)
    failed_set = read_user_agent_set(FAILED_UA_LOG)

    # Only update sets if there's a change
    changed = False
    if success:
        if ua_string not in success_set:
            success_set.add(ua_string)
            failed_set.discard(ua_string)
            write_user_agent_set(SUCCESS_UA_LOG, success_set)
            changed = True
    else:
        if ua_string not in failed_set:
            failed_set.add(ua_string)
            success_set.discard(ua_string)
            write_user_agent_set(FAILED_UA_LOG, failed_set)
            changed = True

    if changed:
        update_valid_user_agents(success_set, failed_set)


def get_valid_user_agents():
    return list(read_user_agent_set(VALID_UA_LOG))