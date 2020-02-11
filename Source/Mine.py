import os.path
import codecs
import multiprocessing as mp
from os import listdir


def _find_matching_patterns_for_line(line, regex_dict):
    results = {}
    for key in regex_dict:
        matches = regex_dict[key].findall(line)
        if matches:
            results[key] = matches
    return results


def _open_file_and_mine_lines(file_path, regex):
    results = []
    with codecs.open(file_path, 'r', encoding='utf8') as fin:
        for line in fin:
            result = _find_matching_patterns_for_line(line, regex)
            if result:
                results.append(result)
    return results


def _mine_logs_from_file(file_path, regex):
    return _open_file_and_mine_lines(file_path, regex)


def _mine_logs_from_dir(dir_path, regex):
    results = []
    for root, subFolders, files in os.walk(dir_path):
        for file in files:
            results.extend(_open_file_and_mine_lines(os.path.join(root, file), regex))

    return results


def mine_logs(file_or_dir_path, regex):
    if os.path.isfile(file_or_dir_path):
        return _mine_logs_from_file(file_or_dir_path, regex)

    elif os.path.isdir(file_or_dir_path):
        return _mine_logs_from_dir(file_or_dir_path, regex)

    else:
        raise OSError


def _parallel_mine_logs_from_dir(dir_path, regex):
    NUMBER_OF_PROCESSES = max(mp.cpu_count() - 1, 1)
    pool = mp.Pool(processes=NUMBER_OF_PROCESSES)
    files = [ os.path.join(dir_path,f) for f in listdir(dir_path) if os.path.isfile(os.path.join(dir_path,f)) ]
    sharded_results = [pool.apply_async(_mine_logs_from_file, args=(file, regex)) for file in files]
    output = [p.get() for p in sharded_results]
    results = []
    for item in output:
        results.extend(item)

    return results


def parallel_mine_logs(file_or_dir_path, regex):
    if os.path.isfile(file_or_dir_path):
        return _mine_logs_from_file(file_or_dir_path, regex)
    elif os.path.isdir(file_or_dir_path):
        return _parallel_mine_logs_from_dir(file_or_dir_path, regex)
    else:
        raise OSError
