"""Utility libraries for deploying containers in AWS ECS."""

import logging
import subprocess

log = logging.getLogger(__name__)

# Required ECS CLI version
ECS_CLI_VERSION = "1.20.0"

def run_cmd(cmd, dry_run=False):
    """Execute ECS command."""
    if dry_run:
        log.info("Dry run mode (Not launching): %s" % " ".join(cmd))
    else:
        subprocess.check_call(cmd)

def validate_ecs_cli_version(ecs_cli_path: str):
    """
    Validate the version of ecs-cli
    """
    ecs_cli_version_str = subprocess.check_output([ecs_cli_path, "--version"],
                                                  text=True)
    # Sample output:
    #   ecs-cli version 1.20.0 (7547c45)
    if f" {ECS_CLI_VERSION} " not in ecs_cli_version_str:
        raise ValueError(f"Need version {ECS_CLI_VERSION} of ecs-cli, "
                           f"found {ecs_cli_version_str}.")


def launch_service_on_ecs(ecs_cli_path,
                          compose_file, project_name,
                          service_state, region='us-east-1',
                          cluster='default', profile='default',
                          project_name_prefix="", service_name_prefix="",
                          dry_run=False, deployment_max_percent=200,
                          elb_arn=None, container_name=None,
                          container_port=None, service_role=None):
    """Launch a Docker container on Amazon ECS.

    Arguments:

    - compose_file: Path to the Docker compose file
    - project_name: Project name to be used.
    - service_state: Requested service state. Acceptable values: 'up', 'down'

    Keyword arguments:

    - region: AWS region to use
    - cluster: ECS cluster to use for service
    - profile: AWS profile to use
    - project_name_prefix: Project name prefix to be used for service name.
      Project name is defined as <project_name_prefix><project_name>.
    - service_name_prefix: Compose service name prefix to be used. Service name
      is defined as <service_name_prefix><project_name>.
    - dry_run: Don't actually run the commands. Just print them
    - deployment_max_percent: Upper limit on running instances while making
      deployment changes. Default value is 200 (%) of desired count.

    ELB related keyword arguments:

    - elb_arn: ARN for the Elastic Load Balancer to be used. Must be created
    before trying to launch service.
    - container_name: Container name to be used with ELB. Needs to be provided
    if trying to attach to an ELB using `elb_arn'.
    - container_port: Container port to be used with ELB. Needs to be provided
    if trying to attach to an ELB using `elb_arn'.
    - service_role: Service role to be used with ELB. Needs to be provided if
    trying to attach to an ELB using `elb_arn'.


    ecs-cli creates the tasks/services needed for running the Grafana
    container given a docker-compose file.

    When ecs-cli creates a task definition, the task definition
    is called ecscompose-<project-name>. Similarly the service is
    called ecscompose-service-<project-name>.
    """
    validate_ecs_cli_version(ecs_cli_path)
    # Need to setup ~/.ecs/config so that our service is deployed to the
    # right cluster
    profile_cmd = [ecs_cli_path, 'configure', 'profile', 'default',
                   '--profile-name', profile]
    run_cmd(profile_cmd, dry_run=dry_run)
    configure_cmd = [ecs_cli_path, 'configure', '-r', region,
                     '-c', cluster,
                     '--compose-service-name-prefix', service_name_prefix]

    run_cmd(configure_cmd, dry_run=dry_run)

    # Now run the CLI to deploy service
    assert service_state in ["up", "down"], \
        "Unknown requested state: %s" % service_state

    launch_cmd = [ecs_cli_path, 'compose', '-f', compose_file,
                  '--project-name', project_name,
                  'service', service_state]

    if service_state == "up":
        launch_cmd += ['--deployment-max-percent', str(deployment_max_percent)]

    if elb_arn and service_state == "up":
        # Also attach to an already existing ELB
        assert container_name and container_port and service_role, \
            "Extra parameters for attaching to ELB are needed"
        launch_cmd += ['--target-group-arn', elb_arn,
                       '--container-name', container_name,
                       '--container-port', "%d" % container_port,
                       '--role', service_role]

    run_cmd(launch_cmd, dry_run=dry_run)


def scale_service_on_ecs(ecs_cli_path,
                         compose_file, project_name, num_instances,
                         region='us-east-1', cluster='default',
                         profile='default', project_name_prefix="",
                         service_name_prefix="", dry_run=False,
                         deployment_max_percent=200):
    """Scale ECS service to requested num of instances.

    Arguments:

    - compose_file: Path to the Docker compose file
    - project_name: Project name to be used.
    - num_instances: Requested number of instances. Specify 0 to shutdown
    service.

    Keyword arguments:

    - region: AWS region to use
    - cluster: ECS cluster to use for service
    - profile: AWS profile to use
    - project_name_prefix: Project name prefix to be used for service name.
    Service name is defined as <project_name_prefix><project_name>.
    - service_name_prefix: Compose service name prefix to be used. Service name
    is defined as <service_name_prefix><project_name>.
    - dry_run: Don't actually run the commands. Just print them
    - deployment_max_percent: Upper limit on running instances while making
    deployment changes. Default value is 200 (%) of desired count.
    """
    validate_ecs_cli_version(ecs_cli_path)
    # Need to setup ~/.ecs/config so that our service is deployed to the
    # right cluster
    profile_cmd = [ecs_cli_path, 'configure', 'profile', 'default',
                   '--profile-name', profile]
    run_cmd(profile_cmd, dry_run=dry_run)
    configure_cmd = [ecs_cli_path, 'configure', '-r', region,
                     '-c', cluster,
                     '--compose-service-name-prefix', service_name_prefix]

    run_cmd(configure_cmd, dry_run=dry_run)

    launch_cmd = [ecs_cli_path, 'compose', '-f', compose_file,
                  '--project-name', project_name, 'service',
                  'scale', str(num_instances),
                  '--deployment-max-percent', str(deployment_max_percent)]

    run_cmd(launch_cmd, dry_run=dry_run)


def push_docker_image_to_ecr_repo(image_name, image_tag, ecr_repo_url,
                                  dry_run=False):
    """Push local Docker image to ECR repository.

    Arguments:
    - image_name: Docker image name
    - image_tag: Docker image tag. The docker image name & tag are combined to
    identify the Docker image locally.
    - ecr_repo_url: The ECR repo to which the Docker image will be pushed. Eg.
    aws_account_id.dkr.ecr.region.amazonaws.com.

    Keyword arguments:
    - dry_run: Don't actually run the commands. Just print them
    """
    target_repo = '%s/%s:%s' % (ecr_repo_url, image_name, image_tag)

    tag_cmd = ['sudo', 'docker', 'tag',
               '%s:%s' % (image_name, image_tag), target_repo]
    push_cmd = ['sudo', 'docker', 'push', target_repo]

    try:
        run_cmd(tag_cmd, dry_run=dry_run)
        run_cmd(push_cmd, dry_run=dry_run)
    except subprocess.CalledProcessError:
        log.info('Re-authenticate docker client to AWS ecr registry')
        get_login_cmd = 'aws ecr get-login --no-include-email'
        credentials = subprocess.getoutput(get_login_cmd)
        if not credentials.startswith('docker login'):
            raise Exception("Received credentials should start "
                            "with 'docker build'. Received: %s"
                            "quitting" % credentials)

        # This essentially tries to login first to ECR
        subprocess.check_call(['sudo'] + credentials.strip().split(' '))

        # Try the push command again after logging in
        subprocess.check_call(push_cmd)

