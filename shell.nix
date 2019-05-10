with import <nixpkgs> {};
stdenv.mkDerivation {
  name = "kalix-env";
  buildInputs = [ python3 pkgconfig openssl rustup git gnuplot nodejs ];
}
