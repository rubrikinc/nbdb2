"""AnomalyDB deploy script."""
import logging
import os
import subprocess
import tempfile

from shutil import copy, copytree, ignore_patterns, rmtree
import jinja2


log = logging.getLogger(__name__)

SRC_PATH = os.path.dirname(os.path.abspath(__file__))
compose_file = os.path.join(SRC_PATH, 'compose', 'flask-local.yml')
compose_template = os.path.join(SRC_PATH, 'compose', "flask-local.yml.j2")


class AnomalyDBDeploy():
    """AnomalyDB deploy class."""

    def stage_files(self):
        """Stage the alert container files."""
        staging_dir = os.path.join(tempfile.mkdtemp(), 'anomalydb')

        # First copy all AnomalyDB files
        copytree(SRC_PATH, staging_dir,
                 ignore=ignore_patterns('*.pyc', '*.sh', 'test'))

        # Copy extra directories for important libraries
        dirs_to_copy = [os.path.join(SRC_PATH, '..', 'nbdb')]
        for src_dir in dirs_to_copy:
            src_path = os.path.join(SRC_PATH, src_dir)
            dst_path = os.path.join(staging_dir, os.path.basename(src_dir))
            copytree(src_path, dst_path,
                     ignore=ignore_patterns('*.pyc', '*.sh', 'test'))

        # Copy necessary extra files
        extra_files = [os.path.join(SRC_PATH, '..', 'setup.py')]
        for f in extra_files:
            src_path = os.path.join(SRC_PATH, f)
            copy(src_path, staging_dir)

        return staging_dir

    def render_jinja_file(self, input_file, output_file, render_kwargs=None,
                          undefined=None):
        render_kwargs = render_kwargs or {}
        path, filename = os.path.split(input_file)
        if undefined:
            # The caller expects to render undefined variables a certain way.
            # Fulfill their expectations
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(path),
                                     undefined=undefined)
        else:
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(path))

        text = env.get_template(filename).render(env=os.environ,
                                                 **render_kwargs)
        with open(output_file, 'w') as f:
            f.write(text)

    def docker_build(self, staging_dir, image_tag=None):
        # Save the current working dir
        cwd = os.getcwd()
        os.chdir(staging_dir)

        build_cmd = ['sudo', 'docker', 'build', '-t',
                     'anomalydb:%s' % image_tag, '-f', 'Dockerfile', '.']
        print(build_cmd)
        try:
            subprocess.check_call(build_cmd, stderr=subprocess.STDOUT)
        finally:
            # Revert back to old working dir and remove the staged files
            os.chdir(cwd)
            rmtree(staging_dir)

    def docker_run(self, image_tag):
        compose_params = {
            'image_repo': 'anomalydb',
            'image_tag': image_tag,
        }

        self.render_jinja_file(compose_template, compose_file,
                               compose_params)
        launch_cmd = ['sudo', 'docker-compose', '-p', 'anomalydb',

                      '-f', compose_file, "up"]
        sub = subprocess.Popen(launch_cmd)
        print("\n\nPress Ctrl-C to stop Heartbeat container ...")

        try:
            sub.wait()
        except KeyboardInterrupt:
            print("\n\nStopping Heartbeat\n\n")
            sub.wait()

    def get_git_hash(self):
        return subprocess.getoutput("git rev-parse HEAD")[:-1]

    def get_image_tag(self):
        return '%s-%s' % ('local', self.get_git_hash())

    def run(self):
        # Build image and launch on aws
        image_tag = self.get_image_tag()
        self.docker_build(self.stage_files(), image_tag)
        self.docker_run(image_tag)


if __name__ == '__main__':
    # Parse command line arguments
    AnomalyDBDeploy().run()
