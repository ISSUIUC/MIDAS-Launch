from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any

import lark
from lark import Lark
from lark.visitors import Interpreter, v_args
from pathlib import Path
import struct

__all__ = ['parse_file', 'Context', 'Type', 'Struct', 'Enum', 'Float', 'Union', 'Integer']

# ebnf grammars are cool!
grammar = r"""
start: top_level*

?top_level: struct | templated_struct | templated_substituted_struct | enum | function | typedef

typedef: "typedef" type IDENTIFIER ";"

function: type IDENTIFIER parenthesized "const"? block

template: "template" "<" _template_param_list ">"
_template_param_list: ((template_param ",")* template_param)?
?template_param: "typename" IDENTIFIER -> template_param_type
            | type IDENTIFIER -> template_param_const
templated_struct: template "struct" IDENTIFIER struct_items ";"
templated_substituted_struct: template "struct" IDENTIFIER "<" template_args ">" struct_items ";"
struct: "struct" IDENTIFIER struct_items ";"

struct_items: "{" struct_item* "}"
struct_item: type field_decl ("," field_decl)* ";" -> fields
           | IDENTIFIER parenthesized ("=" "default" | initializer_list block) ";"? -> constructor
           | "static"? type IDENTIFIER parenthesized "const"? block -> method

field_decl: decl (("=" (const_expr | block)) | block)?
decl: IDENTIFIER ("[" const_expr "]")*

initializer_list: (":" (initializer ",")* initializer)?
initializer: IDENTIFIER parenthesized

?const_expr_0: number
             | IDENTIFIER ("::" IDENTIFIER)* -> ident
             | "(" const_expr ")"
             | "sizeof" "(" type ")" -> sizeof

?const_expr_1: const_expr_1 "+" const_expr_0 -> addition
             | const_expr_1 "-" const_expr_0 -> subtraction
             | const_expr_0

?const_expr: const_expr_1

enum: "enum" "class"? IDENTIFIER "{" _enum_variants "}" ";"
_enum_variants: (enum_variant ("," enum_variant)* ","?)?
enum_variant: IDENTIFIER ("=" number)?

?inner: bracketed | parenthesized | block | TOKEN | ";"
bracketed: "[" inner* "]"
parenthesized: "(" inner* ")"
block: "{" inner* "}"

?type: IDENTIFIER -> type_name
     | IDENTIFIER "<" template_args ">" -> type_generic
     | anonymous_union
     | type "&"
     | type "*"
     | "const" type
template_args: (template_arg ("," template_arg)*)?
?template_arg: type | const_expr
anonymous_union: "union" "{" union_variant+ "}"
union_variant: type IDENTIFIER ";"

%import common.ESCAPED_STRING -> STRING
IDENTIFIER: /[a-zA-Z_][a-zA-Z_0-9]*/

DECIMAL: /[0-9]+/
FLOAT: /[0-9]+.[0-9]+/
BINARY: /0b[0-9]+/
HEX: /0x[0-9]+/
OCT: /0o[0-9]+/

number: DECIMAL | BINARY | HEX | OCT | FLOAT

TOKEN: IDENTIFIER
     | STRING
     | DECIMAL | BINARY | HEX | OCT
     | "." | "," | "?" | "/" | "=" | "+"
     | "-" | "*" | "~" | "!" | "%" | "^"
     | "&" | "|" | ":" | "<" | ">"

%import common.C_COMMENT
%import common.CPP_COMMENT
%import common.WS
%ignore C_COMMENT
%ignore CPP_COMMENT
%ignore WS
"""


class Type(ABC):
    """
    An abstract class which represents Types in C++. As such, it stores a size and alignment, which is all we
    care about. Furthermore, it requires that deriving classes implement a "parse" method, which must extract
    information from the bytes representing this types into json.
    """

    def __init__(self, size: int, align: int):
        self.size = size
        self.align = align

    @staticmethod
    def _calc(types: list[Type]) -> tuple[int, int]:
        """
        Given a list of types, return the total size and the required alignment for that sequence of types.

        Total size is more than just the sum of sizes of the types, since we must account for alignment requirements.
        Required alignment is just the alignment of the most aligned element in the types.

        :param types: A list of Type instances
        :return: A tuple of (total size, required alignment)
        """
        idx = 0
        max_align = 1
        for typ in types:
            size, align = typ.size, typ.align
            if idx % align != 0:
                idx += align - idx % align
            idx += size
            if align > max_align:
                max_align = align
        if idx % max_align != 0:
            idx += max_align - idx % max_align
        return idx, max_align

    @staticmethod
    def _get_parts(data: bytes, types: list[Type]) -> list[bytes]:
        """
        Given some bytes and a list of types that the bytes represent, yield a list of bytes, each of which corresponds
        to a type in the list of types. This function takes into account padding bytes used for alignment and ignores them.

        :param data: Some bytes, which must be exactly as large as the size used to represent `types`
        :param types: A list of types, whose data is stored by `data`
        :return: A list of bytes, which correspond to ranges in `data` for each type in `types`
        """
        idx = 0
        parts = []
        for typ in types:
            size, align = typ.size, typ.align
            if idx % align != 0:
                idx += align - idx % align
            parts.append(data[idx:idx + size])
            idx += size
        return parts

    @abstractmethod
    def parse(self, data: bytes):
        """
        Given some data, parse it as if it represents this type and return the result as JSON

        :param data: The data given. Must be exactly the same length as this type's `size`
        """
        ...

    @abstractmethod
    def get_schema(self) -> Any:
        ...

    def __repr__(self):
        return f"{self.__class__.__name__}(size={self.size}, align={self.align})"


class Union(Type):
    def __init__(self, variants: dict[str, Type]):
        self.variants = variants

        size = max(variant.size for variant in self.variants.values())
        align = max(variant.align for variant in self.variants.values())
        super().__init__(size, align)

    def parse(self, data: bytes):
        parsed = {}
        for variant_name, variant in self.variants.items():
            parsed[variant_name] = variant.parse(data[:variant.size])
        return parsed

    def get_schema(self) -> Any:
        return {"type": "union", "variants": {name: ty.get_schema() for name, ty in self.variants.items()}}

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return f"Union(size={self.size}, align={self.align}, variants={self.variants!r})"


class Struct(Type):
    def __init__(self, members: dict[str, Type]):
        self.members = members
        size, align = self._calc(list(self.members.values()))  # structs are represented as their fields' types in order
        # conveniently, python's dicts are order-preserving, which is needed for the above line to do the right thing
        super().__init__(size, align)

    def parse(self, data: bytes):
        parsed = {}
        parts = self._get_parts(data, list(self.members.values()))  # slice the given data into parts corresponding to
        #  each field, then let each field parse its data
        for (name, typ), part in zip(self.members.items(), parts):
            parsed[name] = typ.parse(part)
        return parsed

    def get_schema(self) -> Any:
        return {"type": "struct", "members": {name: ty.get_schema() for name, ty in self.members.items()}}

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return f"Struct(size={self.size}, align={self.align}, fields={self.members})"


class Array(Type):
    def __init__(self, item: Type, count: int):
        self.item = item
        self.count = count
        size, align = self._calc([item] * count)  # arrays are just their singular item repeated some amount
        super().__init__(size, align)

    def parse(self, data: bytes):
        parsed = [self.item.parse(item) for item in self._get_parts(data, [self.item] * self.count)]
        return parsed

    def get_schema(self) -> Any:
        return {"type": "array", "item": self.item.get_schema(), "count": self.count}

    def __eq__(self, other):
        return isinstance(other, Array) and self.item == other.item and self.count == other.count


class Enum(Type):
    def __init__(self, variants: dict[int, str]):
        self.variants = variants
        # unless otherwise specified, pretty much every compiler makes enums backed by a uint32_t by default
        # so we use the characteristics of a uint32_t
        super().__init__(4, 4)

    def parse(self, data: bytes):
        as_int = struct.unpack("i", data)[0]
        # having accessed the integer value, we map it to its name, which is stored in the `variants` field
        # return as_int
        return self.variants[as_int]

    def get_schema(self) -> Any:
        return {"type": "enum", "variants": {name: value for value, name in self.variants.items()}}

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return f"Enum(variants={self.variants})"


class Boolean(Type):
    def __init__(self):
        # booleans are represented by a single byte, even though they only need a single bit technically
        # this is because our computers have limitations
        super().__init__(1, 1)

    def parse(self, data: bytes):
        # turns out that `?` is the code for a boolean, so this works
        return struct.unpack("?", data)[0]

    def get_schema(self) -> Any:
        return {"type": "bool"}

    def __eq__(self, other):
        return isinstance(other, Boolean)


class Integer(Type):
    def __init__(self, size: int, signed: bool = True, align: int = None):
        # The only case where align is different is with some nonsense involving XMM registers, but oh well
        super().__init__(size, size if align is None else align)
        self.signed = signed
        # only a limited set of sizes is supported, as seen by the below table
        if size == 1 and signed:
            self._format = "b"
        elif size == 1 and not signed:
            self._format = "B"
        elif size == 2 and signed:
            self._format = "h"
        elif size == 2 and not signed:
            self._format = "H"
        elif size == 4 and signed:
            self._format = "i"
        elif size == 4 and not signed:
            self._format = "I"
        elif size == 8 and signed:
            self._format = "q"
        elif size == 8 and not signed:
            self._format = "Q"
        else:
            raise Exception(size, signed)

    def parse(self, data: bytes):
        return struct.unpack(self._format, data)[0]

    def get_schema(self) -> Any:
        return {"type": "int", "signed": self.signed, "size": self.size}

    def __eq__(self, other):
        return isinstance(other, Integer) and self._format == other._format


class Float(Type):
    def __init__(self, size: int, align: int = None):
        super().__init__(size, size if align is None else align)
        # only floats and doubles are supported because python only supports floats and doubles
        # and the code to extract them would be a colossal pain to write
        if size == 4:
            self._format = "f"
        elif size == 8:
            self._format = "d"
        else:
            raise Exception()

    def parse(self, data: bytes):
        return struct.unpack(self._format, data)[0]

    def get_schema(self) -> Any:
        return {"type": "float", "size": self.size}

    def __eq__(self, other):
        return isinstance(other, Float) and self._format == other._format


class TemplateParam:
    def __init__(self, name: str, is_type: bool, default: Type | int | None):
        self.name = name
        self.is_type = is_type
        self.default = default

    def __repr__(self):
        return f"TemplateParam({self.name=}, {self.is_type=}, {self.default=})"


class Template:
    """
    Generated whenever a templated struct is encountered. It stores what template parameters it requires (what type and
    what name they have), as well as the Context it was in and the fields of the struct (as AST).
    """

    def __init__(self, params: dict[str, TemplateParam], fields: list[lark.Tree], ctxt: Context):
        """
        :param params: A list of parameter info tuples. The first item of the tuple should be either the string
        "typename" if it expects a type, or anything else otherwise. The second item of the tuple should be the name the
        parameter has. So template<typename T> would have a params of [("typename, "T")]
        :param fields: The fields of the templated struct, preserved in their AST node form. This makes substitution of
        template parameters really simple. The "field" node of the AST should be passed in.
        :param ctxt: A clone of the context right before the templated struct was evaluated. It shouldn't have the
        templated struct in it.
        """
        self.params = params
        self.fields = fields
        self.ctxt = ctxt

        self.variants: list[Template] = []

    def resolve(self, args: list[lark.Tree], calc: Calculate) -> Callable[[], Struct] | None:
        for variant in reversed(self.variants):
            if (res := variant.resolve(args, calc)) is not None:
                return res

        if len(args) != len(self.params):
            return None
        resolve_ctxt = self.ctxt.clone()
        for arg, param in zip(args, self.params.values()):
            if param.is_type:
                if calc.can_be_type(arg):
                    typ = calc.as_type(arg)
                    if param.default is not None and param.default != typ:
                        return None
                    resolve_ctxt.types[param.name] = typ
                else:
                    return None
            else:
                if calc.can_be_const_expr(arg):
                    val = calc.as_const_expr(arg)
                    if param.default is not None and param.default != val:
                        return None
                    resolve_ctxt.names[param.name] = val
                else:
                    return None

        def render() -> Struct:
            new_calc = Calculate(resolve_ctxt)
            fields: dict[str, Type] = {}
            for item in self.fields:
                f_name, field = new_calc.visit(item)
                fields[f_name] = field
            return Struct(fields)

        return render


@dataclass
class Context:
    """
    A convenience class which binds together the three aspects tracked in a namespace (for our purposes), that being
    constants, types, and templates.
    """

    names: dict[str, int]
    types: dict[str, Type]
    templates: dict[str, Template]

    def clone(self) -> Context:
        """
        Creates a copy of the context, so we can change one without affecting the other.
        """
        return Context(self.names.copy(), self.types.copy(), self.templates.copy())


class Calculate(Interpreter):
    """
    The primary workhorse class, it takes in AST nodes and a Context, and outputs into that Context the data types
    that were defined in the AST nodes.
    """

    def __init__(self, ctxt: Context):
        """
        :param ctxt: The context to evaluate the provided AST nodes in. This will be mutated.
        """
        self.ctxt = ctxt
        self.names = ctxt.names
        self.types = ctxt.types
        self.templates = ctxt.templates

    # The way Lark works is that (for classes inheriting from Interpreter), the `visit` method defined on Interpreter
    # takes a lark.Tree or a lark.Token and calls the method with the same name as the rule that created said
    # Tree or Token (that field which defines the rule which created it is called `data` on Trees and 'type' on Tokens).

    def __default__(self, node):
        raise Exception(node)

    def start(self, top_levels: lark.Tree):
        # the `start` node contains all the structs and enums in the file, so we only need to visit each child node
        # which lark provides a helper for
        self.visit_children(top_levels)

    def function(self, tree):
        pass

    @v_args(inline=True)
    def enum(self, name: lark.Token, *variants: lark.Tree):
        # the @v_args(inline=True) means that the children of the `enum` node will be provided as arguments, instead of
        # the `enum` node itself being provided as the argument as would happen normally

        enum: dict[int, str] = {}  # stores the variants of the enum as a mapping from value to the name of the variant
        recent = 0
        for variant in variants:
            # calls the `enum_variant` method, which returns the name of the variant as a str
            variant_name, value = self.visit(variant)
            if value is not None:
                recent = value
            enum[recent] = variant_name
            recent += 1

        # having collected a map from the number which represents each variant to the variant's name, make it into an
        # Enum type and put it in this context's type map
        self.types[str(name)] = Enum(enum)

    @v_args(inline=True)
    def enum_variant(self, name: lark.Token, value: lark.Tree = None) -> tuple[str, int | None]:
        return str(name), self.visit(value) if value is not None else None

    @v_args(inline=True)
    def typedef(self, ty: lark.Tree, name: lark.Token):
        self.types[str(name)] = self.visit(ty)

    @v_args(inline=True)
    def struct(self, name: lark.Token, items: lark.Tree):
        # `struct` takes a name and a container of `struct_items`. It then finds the type of each field and stores the
        # resulting `Struct`
        fields: dict[str, Type] = {}
        for item in items.children:
            if item.data == "fields":
                for f_name, field in self.visit(item):
                    fields[f_name] = field
            elif item.data == "method":
                continue
            elif item.data == "constructor":
                continue
            else:
                raise Exception(item.data)
        self.types[str(name)] = Struct(fields)

    @v_args(inline=True)
    def template_param_type(self, name: lark.Token) -> TemplateParam:
        return TemplateParam(str(name), True, None)

    @v_args(inline=True)
    def template_param_const(self, typ: lark.Tree, name: lark.Token) -> TemplateParam:
        return TemplateParam(str(name), False, None)

    @v_args(inline=True)
    def templated_struct(self, template: lark.Tree, name: lark.Token, items: lark.Tree):
        template_params: dict[str, TemplateParam] = {}
        for template_param in template.children:
            param: TemplateParam = self.visit(template_param)
            template_params[param.name] = param

        fields: list[lark.Tree] = []
        for item in items.children:
            if item.data == "field":
                fields.append(item)
        self.templates[str(name)] = Template(template_params, fields, self.ctxt)

    @v_args(inline=True)
    def templated_substituted_struct(self, template: lark.Tree, name: lark.Token, substitution: lark.Tree,
                                     items: lark.Tree):
        variant_of = self.templates[str(name)]
        if len(variant_of.params) != len(substitution.children):
            raise Exception()

        template_params: dict[str, TemplateParam] = {}
        for template_param in template.children:
            param: TemplateParam = self.visit(template_param)
            template_params[param.name] = param

        actual_template_params: dict[str, TemplateParam] = {}
        for actual_param, item in zip(variant_of.params.values(), substitution.children):
            if item.data in ("type_name", "ident"):
                name = str(item.children[0])
                if name in template_params.keys():
                    actual_template_params[actual_param.name] = template_params[name]
                    continue

            if actual_param.is_type:
                if self.can_be_type(item):
                    typ = self.as_type(item)
                    actual_template_param = TemplateParam(actual_param.name, True, typ)
                else:
                    raise Exception()
            else:
                if self.can_be_const_expr(item):
                    val = self.as_const_expr(item)
                    actual_template_param = TemplateParam(actual_param.name, False, val)
                else:
                    raise Exception()
            actual_template_params[actual_param.name] = actual_template_param

        fields: list[lark.Tree] = []
        for item in items.children:
            if item.data == "field":
                fields.append(item)
        template = Template(actual_template_params, fields, self.ctxt)
        variant_of.variants.append(template)

    @v_args(inline=True)
    def fields(self, field_type: lark.Tree, *decls: lark.Tree) -> list[tuple[str, Type]]:
        fields = []
        for field_decl in decls:
            decl = field_decl.children[0]
            fields.append(self.decl(field_type, *decl.children))
        return fields

    def decl(self, typ: lark.Tree, name: lark.Token, *arrays: lark.Tree) -> tuple[str, Type]:
        base: Type = self.visit(typ)  # the left hand side of the type (the type without any array info)
        for array in arrays:  # for each array part (the [<const_expr>] part), wrap in another Array
            count: int = self.visit(array)  # visiting resolves the const_expr into an actual number we can use
            base = Array(base, count)
        return str(name), base

    @staticmethod
    def can_be_type(tree: lark.Tree) -> bool:
        return tree.data in ("ident", "type_name", "type_generic")

    @staticmethod
    def can_be_const_expr(tree: lark.Tree) -> bool:
        return tree.data in ("ident", "type_name", "number", "sizeof", "addition", "subtraction")

    def as_type(self, tree: lark.Tree) -> Type:
        if self.can_be_type(tree):
            return self.visit(tree)
        else:
            raise Exception()

    def as_const_expr(self, tree: lark.Tree) -> int:
        if self.can_be_const_expr(tree):
            if tree.data == "type_name":
                return self.ident(*tree.children)
            else:
                return self.visit(tree)
        else:
            raise Exception()

    @v_args(inline=True)
    def ident(self, name: lark.Token) -> int:
        # straightforward, if we see a bare name as a const_expr, fetch the value of it
        # useful where an array count is a template parameter
        return self.names[str(name)]

    @v_args(inline=True)
    def number(self, num: lark.Token) -> int:
        if num.type == "DECIMAL":
            return int(num.value)
        elif num.type == "HEX":
            return int(num.value[2:], 16)
        elif num.type == "BINARY":
            return int(num.value[2:], 2)
        elif num.type == "OCT":
            return int(num.value[2:], 8)
        else:
            raise Exception()

    @v_args(inline=True)
    def addition(self, left: lark.Tree, right: lark.Tree) -> int:
        return self.visit(left) + self.visit(right)

    @v_args(inline=True)
    def subtraction(self, left: lark.Tree, right: lark.Tree) -> int:
        return self.visit(left) - self.visit(right)

    @v_args(inline=True)
    def sizeof(self, typ: lark.Tree) -> int:
        resolved_type: Type = self.visit(typ)
        return resolved_type.size

    @v_args(inline=True)
    def type_name(self, name: lark.Token) -> Type:
        # if we see a name used as a type, fetch the appropriate type
        return self.types[str(name)]

    @v_args(inline=True)
    def type_generic(self, name: lark.Token, args: lark.Tree) -> Type:
        renderer_or_none = self.templates[str(name)].resolve(args.children, self)
        if renderer_or_none is None:
            raise Exception()
        else:
            return renderer_or_none()

    @v_args(inline=True)
    def anonymous_union(self, *union_variants: lark.Tree) -> Type:
        variants: dict[str, Type] = {}
        for union_variant in union_variants:
            name, typ = self.visit(union_variant)
            variants[name] = typ
        return Union(variants)

    @v_args(inline=True)
    def union_variant(self, typ: lark.Tree, name: lark.Token) -> tuple[str, Type]:
        return str(name), self.visit(typ)


# this stuff is global for easier editing, I guess
# look at main to see how it's used
BASE_NAMES: dict[str, int] = {}
BASE_TYPES: dict[str, Type] = {
    'bool': Boolean(),
    'char': Integer(1),
    'float': Float(4),
    'int': Integer(4, signed=False),
    'uint16_t': Integer(4, signed=False),
    'uint32_t': Integer(4, signed=False),
    'uint64_t': Integer(8, signed=False),
    'int16_t': Integer(4, signed=True),
    'int32_t': Integer(4, signed=True),
    'int64_t': Integer(8, signed=True),
    # 'systime_t': Integer(4)
}
BASE_TEMPLATES: dict[str, Template] = {}
BASE_CTXT = Context(BASE_NAMES, BASE_TYPES, BASE_TEMPLATES)

STD_HEADERS = {
    "<cmath>": "",
    "<cstdint>": "",
    "<algorithm>": "",
    "<cstring>": "",
    "<string>": "",
    "<string.h>": "",
    "<math.h>": ""
}


class Preprocessor:
    def __init__(self):
        self.included_files: dict[Path, str] = {}
        self.pragma_once: set[str] = set()

    def include_file(self, path: Path) -> str:
        path = path.absolute()
        if path not in self.included_files:
            text = path.read_text()
            self.included_files[path] = text
        return self.preprocess(str(path), self.included_files[path])

    def preprocess(self, file_path: str, text: str) -> str:
        processed = ""
        for i, line in enumerate(text.splitlines()):
            if line.startswith("#"):
                parts = line.split()
            else:
                processed += line + "\n"
                continue

            if parts[:2] == ["#pragma", "once"]:
                if file_path in self.pragma_once:
                    return ""
                else:
                    self.pragma_once.add(file_path)
            elif parts[:1] == ["#include"]:
                if len(parts) != 2:
                    raise Exception(f"Malformed line {i+1} of file {file_path}")

                include_path = parts[1]

                if include_path.startswith("\"") and include_path.endswith("\""):
                    include_path = Path(file_path).absolute().parent / include_path[1:-1]
                    processed += self.include_file(include_path)
                elif include_path in STD_HEADERS:
                    processed += self.preprocess(include_path[1:-1], STD_HEADERS[include_path])
                else:
                    raise Exception(f"Malformed line {i+1} of file {file_path}")
            else:
                raise Exception(f"Malformed line {i + 1} of file {file_path} (directive not supported yet)")
            processed += "\n"
        return processed


def parse_file(file: Path) -> Context:
    preprocessor = Preprocessor()
    text = preprocessor.include_file(file)

    # print(text)

    parser = Lark(grammar, parser="earley")
    # parse the text into an AST made of lark.Tree and lark.Token
    try:
        tree = parser.parse(text)
    except Exception as e:
        raise Exception(f"Could not parse {file}") from e
    # create the global namespace, containing the default types like `bool` and `int`
    ctxt = BASE_CTXT.clone()
    # create the base Calculate instance for the global namespace, then run it for the AST given
    calc = Calculate(ctxt)
    calc.visit(tree)

    return ctxt

# def render_bytes(n: int) -> str:
#     """
#     A helper function which makes a number of bytes prettier by appending units.
#
#     For example, if given 22200 bytes, this will display it as 22.20 kb.
#
#     :param n: A number of bytes.
#     :return: The number of bytes, with units (up to Tb) and 4 significant digits.
#     """
#     endings = ["", " kb", " Mb", " Gb", " Tb"]
#     end_idx = 0
#     while n >= 1000 and end_idx < len(endings):
#         n /= 1000
#         end_idx += 1
#     return f"{n:#.4g}{endings[end_idx]}"
#
#
# def get_arguments():
#     """
#     A helper function which parses the arguments passed to the file.
#
#     Most notably, if the "out" parameter isn't supplied, it defaults
#     to the "raw" path with the suffix changed to ".json".
#
#     :return: An object which has three fields, each of which contains a pathlib.Path: 'header', 'raw', and 'out'.
#     """
#     parser = ArgumentParser(prog="data_parser")
#     parser.add_argument("header", type=pathlib.Path)
#     parser.add_argument("raw", type=pathlib.Path)
#     parser.add_argument("-o", "--out", type=pathlib.Path)
#     args = parser.parse_args()
#
#     if args.out is None:
#         raw_path: pathlib.Path = args.raw
#         args.out = raw_path.with_suffix(".json")
#     return args

#
# def main():
#     args = get_arguments()
#
#     parser = Lark(grammar, parser="earley")
#     # open up the header file and read it in
#     with args.header.open("r") as f:
#         text = f.read()
#     # parse the text into an AST made of lark.Tree and lark.Token
#     tree = parser.parse(preprocess(text))
#     # create the global namespace, containing the default types like `bool` and `int`
#     ctxt = BASE_CTXT.clone()
#     # create the base Calculate instance for the global namespace, then run it for the AST given
#     calc = Calculate(ctxt)
#     calc.visit(tree)
#
#     # extract the info for the sensorDataStruct_t from the context (since Calculate mutates it as it goes)
#     sensor_data_struct: Type = ctxt.types["sensorDataStruct_t"]
#
#     # open up the raw data file and read it in
#     with args.raw.open("rb") as f_da:
#         all_data: bytes = f_da.read()
#
#     idx = 0  # a counter which keeps track of how far into the raw data we are
#     # open up the output file, which we write to incrementally
#     with args.out.open("w") as f_out:
#         f_out.write("[")
#         prev = False  # keeps track of whether this is the first line written, so we know whether to prepend a comma
#         while idx < len(all_data):
#             # read in a slice of data equal to the size of sensorDataStruct_t, then increment the counter by that much
#             raw = all_data[idx:idx + sensor_data_struct.size]
#             idx += sensor_data_struct.size
#             # if there wasn't enough data to read, end the loop
#             if len(raw) < sensor_data_struct.size:
#                 break
#
#             # use the Type.parse method in order to convert the raw binary to json
#             out = sensor_data_struct.parse(raw)
#             if prev:
#                 f_out.write(",\n")
#             else:
#                 f_out.write("\n")
#                 prev = True
#             # write the json to the output file
#             f_out.write(json.dumps(out))
#             # displays the progress
#             print(f"\rParsed {render_bytes(idx)}/{render_bytes(len(all_data))} bytes", end="", flush=True)
#         f_out.write("\n]")
#
#
# if __name__ == '__main__':
#     main()