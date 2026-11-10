{
  # A throwaway VM with rootful Docker for `make e2e` with every Chaos Mesh
  # experiment. Rootless Docker (and some hosts) can't run NetworkChaos (needs
  # legacy iptables + netem in the node's kernel) or IOChaos (FUSE). Driven by
  # hack/e2e-vm/run.sh; nothing here is used by CI.
  description = "imposm-operator e2e VM (rootful Docker, kind, Chaos Mesh-capable kernel)";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  outputs = { self, nixpkgs }: {
    nixosConfigurations.vm = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ({ pkgs, modulesPath, ... }: {
          imports = [ "${modulesPath}/virtualisation/qemu-vm.nix" ];
          virtualisation = {
            memorySize = 10240;
            cores = 8;
            diskSize = 61440;
            graphics = false;
            docker.enable = true;
            # ssh is forwarded by run.sh (QEMU_NET_OPTS, localhost only)
          };
          # Chaos Mesh needs legacy iptables, which 6.18 builds without
          # (CONFIG_NETFILTER_XTABLES_LEGACY unset): use the 6.12 LTS kernel.
          boot.kernelPackages = pkgs.linuxPackages_6_12;
          # Chaos Mesh: legacy iptables + netem; kind: bridge netfilter.
          boot.kernelModules = [ "ip_tables" "iptable_filter" "iptable_mangle" "iptable_nat" "sch_netem" "br_netfilter" ];
          boot.kernel.sysctl = {
            "fs.inotify.max_user_instances" = 1024;
            "fs.inotify.max_user_watches" = 1048576;
          };
          services.openssh = { enable = true; settings.PermitRootLogin = "prohibit-password"; };
          # The ssh key is generated per user by run.sh and reaches the VM via
          # the default /tmp/shared 9p share ($SHARED_DIR), so it never has to
          # be part of the flake.
          systemd.services.e2e-authorized-key = {
            wantedBy = [ "multi-user.target" ];
            before = [ "sshd.service" ];
            after = [ "local-fs.target" ];
            serviceConfig.Type = "oneshot";
            script = ''
              install -d -m 700 /root/.ssh
              install -m 600 /tmp/shared/id.pub /root/.ssh/authorized_keys
            '';
          };
          networking.firewall.enable = false;
          environment.systemPackages = with pkgs; [ kind kubernetes-helm kubectl git curl python3 gnumake rsync gzip ];
          system.stateVersion = "25.11";
        })
      ];
    };
  };
}
