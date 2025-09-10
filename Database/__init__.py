#!/usr/bin/env python3
"""
Inicialización del módulo Database
==================================

Este módulo expone las funciones principales para trabajar con la base de datos.
"""

from .connection import (
    db_manager,
    get_session,
    get_engine,
    create_all_tables,
    test_db_connection
)

# Base se importa desde Models para evitar circular imports
def get_base():
    """Obtiene la Base declarativa de manera lazy."""
    from Models.base import Base
    return Base

__all__ = [
    'db_manager',
    'get_session', 
    'get_engine',
    'create_all_tables',
    'test_db_connection',
    'get_base'
]
