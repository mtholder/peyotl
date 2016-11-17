#!/usr/bin/env python
"""Functions for validating amendment JSON.
"""
from peyotl.amendments.validation.adaptor import create_validation_adaptor


def validate_amendment(obj, **kwargs):
    """Takes an `obj` that is an amendment object.
    Returns the pair:
        errors, adaptor
    `errors` is a simple list of error messages
    `adaptor` will be an instance of amendments.validation.adaptor.AmendmentValidationAdaptor
        it holds a reference to `obj` and the bookkeepping data necessary to attach
        the log message to `obj` if
    """
    # Gather and report errors in a simple list
    errors = []
    n = create_validation_adaptor(obj, errors, **kwargs)
    return errors, n
