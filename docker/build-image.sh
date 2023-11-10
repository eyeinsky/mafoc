echo Build docker

set -e

target="mafoc:exe:${1:-mafoc}"; echo "cabal target: $target"
tmp_nix_store=./runtime-dependencies

buildInNix() (
    component="$1"
    dest="$2"
    nix develop --command bash -c "cabal build $component && ln \"\$(cabal list-bin $component)\" \"$dest\""
)

exe_name="$(echo "$target" | awk -F: '{print $3}')"
exe="$(mktemp -u "${exe_name:?}.XXXXXXXX")"

buildInNix "$target" "$exe"

if stat "$exe" >/dev/null; then
    rsync -a --delete --stats --chmod=u+w \
          --files-from <(nix-exe-dependency-closure.hs "$exe" | xargs find) / "$tmp_nix_store"
    docker build \
           -f docker/Dockerfile \
           --build-arg NIX_STORE="${tmp_nix_store:?}" \
           --build-arg EXE="${exe:?}" \
           .
    rm "$exe"
else
    echo exe not found
fi
