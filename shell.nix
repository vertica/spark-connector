{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  buildInputs = with pkgs; [
    jdk8
    wget
    unzip
    sbt
    vim
    bash
  ];
  shellHook = ''
  '';
}
