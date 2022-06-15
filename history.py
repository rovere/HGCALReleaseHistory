#!/usr/bin/env python

import sh
import re
import fire
import multiprocessing

# https://stackoverflow.com/questions/5967500/how-to-correctly-sort-a-string-with-a-number-inside
def atoi(text):
    return int(text) if text.isdigit() else text

def natural_tag_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''

    if not re.match(r'.*pre[0-9]+$', text):
        text = text+'_stable0'
        print("Correcting tag", text)

    return [ atoi(c) for c in re.split(r'(\d+)', text)]

def writeGraphPreamble_(label, output_file):
    preamble="""
    label="{}"
    labelloc="t"
    fontname="Ubuntu"
    fontsize=16
    graph [rankdir="RL",
        overlap=true,
        splines=false,
        nodesep=0.3,
        ranksep=0.2,
        bgcolor="transparent",
    ];
    node [fixedsize=true,
        fontname="Ubuntu"
        fontsize=12,
        shape=box,
        style="filled,setlinewidth(2)",
        width=3.4,
        height=0.4,
    ];
    edge [arrowhead=none,
        arrowsize=0.5,
        style=invis,
        labelfontname="Ubuntu",
        weight=10,
        style="filled,setlinewidth(2)"
    ];
    """

    output_file.write("{}\n".format(preamble.format(label.strip())))

def writeCommit_(commits, output_file):
    commits = commits.stdout.decode().strip().rsplit('\n')
    for c in commits:
        # Merge pull request #28109 from davidlange6/rawlzma (2019-10-09) <cmsbuild>
        formatted_commit = re.sub(r'.*#(\d+) from (.*) \((.*)\)', '  ghost_\\1 [color="#73d216ff", shape=point, height=0.2];\n  \\1 [color="#73d216ff", URL="https://github.com/cms-sw/cmssw/pull/\\1", label="#\\1 (\\3)\n\\2"];\n  ghost_\\1 -> \\1 [color="#73d216ff"]', c)
        output_file.write("{}\n".format(formatted_commit))

def writeTag_(tag, output_file):
    """Output the tag as a node, eventually with the link if the tag is not a
    'X' development branch"""

    formatstr_tag_with_url = '  {tag}_ghost [color="#fcaf3eff", shape=point, height=0.2]; {tag} [color="#fcaf3eff", URL="https://github.com/cms-sw/cmssw/releases/tag/{tag}"]; {tag}_ghost -> {tag} [color="#fcaf3eff"];\n'
    formatstr_tag_no_url = '  {tag}_ghost [color="#fcaf3eff", shape=point, height=0.2]; {tag} [color="#fcaf3eff"]; {tag}_ghost -> {tag} [color="#fcaf3eff"];\n'
    if re.match(r'.*\d+$', tag):
        output_file.write(formatstr_tag_with_url.format(tag=tag))
    else:
        output_file.write(formatstr_tag_no_url.format(tag=tag))

def formatSVGForMkdocs(filename):
    # Add a target to the links to open them in a separate tab
    sh.sed('-i',
           '-e',
           's#<a xlink#<a target="_blank" xlink#g',
           filename)

    # Remove first 3 lines that are not properly rendered when the SVG is embedded
    sh.sed('-i',
           '1,3d',
           filename)

def processOnePackage(package, packages, workers, release_start, release_end, verbose):
    base_filename='{}-{}-{}'.format(release_start, release_end, package.strip().replace('/','-'))
    process_gv_file=False
    if verbose:
        print("Analysing package {}".format(package))
    with open("{}.gv".format(base_filename), 'w') as output_file:
        output_file.write("digraph git {\n")
        writeGraphPreamble_(package, output_file)
        writeTag_(release_end, output_file)
        if verbose:
            print("Creating history for package {}".format(package))
            print("Running: git --no-pager lg origin/{} ^origin/{} --first-parent".format(release_start, release_end))
        tags = sh.sort(
                sh.grep(
                    sh.git('--no-pager', 'lg', 'origin/'+release_start, '^origin/'+release_end, '--first-parent'),
                    '-w', '-P', '\s+CMSSW_[0-9]+_[0-9]+_[0-9]+(_pre[0-9]+)?', '-o', _ok_code=[0,1]),
                '-u')
        if len(tags.stdout.decode()) != 0:
            process_gv_file = True
            tags = tags.stdout.decode().strip().rsplit('\n')
            tags = [t.strip() for t in tags]
            tags.sort(key=natural_tag_keys)
            not_tags = ['^'+release_end] + ['^'+t for t in tags]
            if verbose:
                print("Sorted tags", tags)
                print("Sorted not_tags", not_tags)
            for tag_start, tag_end in zip(tags, not_tags):
                print("From {} to {}:\n".format(tag_start, tag_end))
                if verbose:
                    print("git " + " ".join(['--no-pager', 'lg', '--first-parent', tag_start, tag_end, '--', package.strip()]))
                commits = sh.git('--no-pager', 'lgh', '--first-parent', tag_start, tag_end, '--', package.strip())
                if commits:
                    writeCommit_(commits, output_file)
                    if verbose:
                        print("Adding commit(s)")
                writeTag_(tag_start, output_file)
            output_file.write("end [label={}]\n".format(release_start))
            output_file.write("}\n")
    if process_gv_file:
        sh.dot('-Tsvg',
                '{}.gv'.format(base_filename),
                '-o{}.svg'.format(base_filename)
               )
        formatSVGForMkdocs('{}.svg'.format(base_filename))
    packages.task_done()
    # Add a worker back on the pool
    if verbose:
        print("Adding back one worker node")
    workers.put(1)


def findMergeCommits(release_start, release_end, package_file, processes=8, verbose=False):

    workers = multiprocessing.Queue(processes)
    for w in range(processes):
        if verbose:
            print("Setting up worker {}".format(w))
        workers.put(w)

    packages = multiprocessing.JoinableQueue(0)
    with open(package_file) as p:
        for package in p.readlines():
            if verbose:
                print("Setting up packages queue with entry {}".format(package))
            packages.put(package)

        if verbose:
            print("Processing a package queue of size {}".format(packages.qsize()))

        if verbose:
            print("Packages queue is empty {}".format(packages.empty()))

        while True:
            package = packages.get()
            w = workers.get()
            if verbose:
                print("Remove {} from the packages queue".format(package))
                print("Available workers: {}".format(workers.qsize()))
            p = multiprocessing.Process(target=processOnePackage, args=(package, packages, workers, release_start, release_end, verbose))
            p.start()
            if packages.empty():
                packages.join()
                break

if __name__ == '__main__':
    fire.Fire(findMergeCommits)
