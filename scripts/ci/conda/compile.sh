#!/bin/bash

mkdir packages


CI_SUPPORT=""
if [ "$BUILD_PLATFORM" == "windows-latest" ]; then
    CI_SUPPORT="win_64_.yaml"
fi

if [ "$BUILD_PLATFORM" == "ubuntu-latest" ]; then
    CI_SUPPORT="linux_64_.yaml"
fi

if [ "$BUILD_PLATFORM" == "macos-latest" ]; then
    CI_SUPPORT="osx_64_.yaml"
fi

conda build recipe --clobber-file recipe/recipe_clobber.yaml --output-folder packages -m .ci_support/$CI_SUPPORT

conda install -c ./packages entwine

entwine --version
