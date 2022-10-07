#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import datetime
import hashlib
from typing import NamedTuple, TYPE_CHECKING

from .extend_validator_helpers import (
	refResolver_resolve,
)
from .extensions.fk_check import ForeignKey
from .extensions.pk_check import PrimaryKey

if TYPE_CHECKING:
	from typing import (
		Any,
		ClassVar,
		IO,
		Iterator,
		NamedTuple,
		Mapping,
		MutableMapping,
		MutableSequence,
		Optional,
		Sequence,
		Set,
		Tuple,
		Type,
		Union,
	)

	from .extensible_validator import ExtensibleValidator

DECO = {
	"object": "{}",
	"array": "[]",
}


class FKEdge(NamedTuple):
	fromNodeId: str
	mport: str
	toNodeId: str


def s_sum(the_str: str) -> str:
	oP = hashlib.sha1()
	oP.update(the_str.encode("utf-8"))
	return oP.hexdigest()


def genObjectNodes(
	label: str,
	kPayload: "Mapping[str, Any]",
	prefix: "Optional[str]",
	pk_set: "Optional[Set[str]]" = None,
	fk_edges: "Optional[Sequence[FKEdge]]" = None,
	schema_id: "Optional[str]" = None,
) -> str:
	if prefix is not None and len(prefix) > 0:
		origPrefix = prefix
		prefix += "."
	else:
		origPrefix = prefix = ""

	# Avoiding special chars
	origPrefix = s_sum(origPrefix)

	origLabel = None
	ret_label: str = label
	# $label =~ s/([\[\]\{\}])/\\$1/g;
	if kPayload.get("type") == "object":
		kAll = copy.copy(kPayload.get("allOf", []))
		kAll.insert(0, kPayload)

		kP = {}
		req = []
		if len(kAll) > 0:
			for kOne in kAll:
				req.extend(kOne.get("required", []))
				kOP = kOne.get("properties")
				if kOP is not None:
					kP.update(kOP)
		if kP:
			origLabel = label
			if schema_id is not None:
				ret_label = f"""
<FONT FACE="Courier">
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" BGCOLOR="white">
	<TR>
		<TD COLSPAN="2" ALIGN="CENTER" PORT="schema" BGCOLOR="lightgreen"><FONT POINT-SIZE="20">{label}</FONT><BR/><FONT POINT-SIZE="8">{schema_id}</FONT></TD>
	</TR>
"""
			else:
				ret_label = f"""
		<TD ALIGN="LEFT" PORT="{origPrefix}">{label}</TD>
		<TD BORDER="0"><TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
"""

			ret = []

			for keyP, valP in kP.items():
				ret.append(
					genNode(
						keyP,
						valP,
						prefix,
						pk_set=pk_set,
						fk_edges=fk_edges,
						required=keyP in req,
					)
				)

			ret_label += "\t<TR>\n" + "\t</TR>\n\t<TR>\n".join(ret) + "\t</TR>\n"

			if schema_id is not None:
				ret_label += "</TABLE></FONT>"
			else:
				ret_label += "</TABLE></TD>\n"

	if origLabel is None:
		# label = f"\t\t<TD COLSPAN=\"2\">{label}</TD>\n"
		ret_label = ""

	return ret_label


def genNode(
	key: str,
	kPayload: "Mapping[str, Any]",
	prefix: "Optional[str]",
	pk_set: "Optional[Set[str]]" = None,
	fk_edges: "Optional[Sequence[FKEdge]]" = None,
	required: bool = False,
) -> str:
	val = key
	if prefix is None:
		prefix = ""
	while "type" in kPayload:
		k_type = kPayload["type"]
		d_k_t = DECO.get(k_type)
		if d_k_t is not None:
			val += d_k_t

		if k_type == "array":
			key += "[]"

			k_items = kPayload.get("items")
			if k_items is not None:
				kPayload = k_items
				continue
		elif k_type == "object":
			k_props = kPayload.get("properties")
			if k_props is not None:
				return genObjectNodes(
					val, kPayload, prefix + key, pk_set=pk_set, fk_edges=fk_edges
				)

		break

	# Escaping
	# $val =~ s/([\[\]\{\}])/\\$1/g;

	# Avoiding special chars
	toHeaderName = s_sum(prefix + key)

	# Labelling the foreign keys
	preval = ""
	if fk_edges is not None:
		for fk_edge in fk_edges:
			if fk_edge.mport == toHeaderName:
				val = f"<I>{val}</I>"
				preval += "\u2387"
				break

	if pk_set is not None and toHeaderName in pk_set:
		required = True
		val = f'<FONT COLOR="BLUE">{val}</FONT>'
		preval += "\U0001F511"

	if required:
		val = f"<B>{val}</B>"

	return (
		f'\t\t<TD ALIGN="LEFT" PORT="{toHeaderName}" COLSPAN="2">{val}{preval}</TD>\n'
	)
	# return f'\t\t<TD ALIGN="LEFT" PORT="{toHeaderName}" SIDES="LTB">{val}</TD><TD ALIGN="RIGHT" SIDES="RTB">{preval}</TD>\n'


def drawSchemasToFile(
	ev: "ExtensibleValidator", output_filename: str, title: str = "JSON Schemas"
) -> int:
	validSchemaDict = ev.getValidSchemas()
	if len(validSchemaDict.keys()) == 0:
		ev.logger.fatal("No schema was successfully loaded, so no drawing is possible")
		return 1
	with open(output_filename, mode="w", encoding="utf-8") as DOT:
		return drawSchemasToStream(ev, DOT, title=title)


def schemaPath2JSONPath(schemaPath: str) -> str:
	jpath = ""
	if len(schemaPath) == 0:
		return jpath

	prevProperties = False
	for token in schemaPath.split("/"):
		if token == "":
			continue
		if prevProperties:
			if len(jpath) > 0:
				jpath += "."
			jpath += token
			prevProperties = False
		elif token == "properties":
			prevProperties = True
		elif token == "items":
			jpath += "[]"
		else:
			print(f"Mira {token} {schemaPath}")

	return jpath


def drawSchemasToStream(ev: "ExtensibleValidator", DOT: "IO[str]", title: str) -> int:
	validSchemaDict = ev.getValidSchemas()
	refSchemaSet = ev.getRefSchemaSet()
	# Now it is time to draw the schemas themselves
	pre = f"""
digraph schemas {{
	graph[ rankdir=LR, ranksep=2, fontsize=60, fontname="Helvetica", labelloc=t, label=< {title} <br/> <font point-size="40">(as of {datetime.datetime.now().isoformat()})</font> >  ];
	node [shape=tab, style=filled, fillcolor="green"];
"""
	# 	node [shape=record];
	DOT.write(pre)

	# First pass
	sCounter = 0
	sHash = dict()

	for jsonSchemaURI, schemaObj in validSchemaDict.items():
		resolved_schema = schemaObj["resolved_schema"]

		nodeId = f"s{sCounter}"

		if "properties" in resolved_schema:
			sHash[jsonSchemaURI] = nodeId
			sCounter += 1

	# Gathering edges for foreign keys
	fk_edges: "MutableSequence[FKEdge]" = []
	fk_edges_d: "MutableMapping[str, MutableSequence[FKEdge]]" = {}
	pk_sets: "MutableMapping[str, Set[str]]" = {}
	for jsonSchemaURI, jsonSchemaSet in refSchemaSet.items():
		fromNodeId = sHash.get(jsonSchemaURI)
		schemaObj_o = validSchemaDict.get(jsonSchemaURI)

		if fromNodeId is None:
			continue

		if schemaObj_o is None:
			continue

		refResolver = schemaObj_o["ref_resolver"]

		id2ElemId, keyRefs, jp2val = jsonSchemaSet
		for the_id, featureLocs in keyRefs.items():
			if the_id == ForeignKey.KeyAttributeNameFK:
				# TO FINISH
				for featureLoc in featureLocs:
					for fk_decl in featureLoc.context[ForeignKey.KeyAttributeNameFK]:
						resolved = refResolver_resolve(
							refResolver, fk_decl["schema_id"]
						)
						if resolved is None:
							continue
						to_jsonSchemaURI = resolved[0]
						toHeaderName = to_jsonSchemaURI
						rSlash = toHeaderName.rfind("/")
						if rSlash != -1:
							toHeaderName = toHeaderName[rSlash + 1 :]

						toNodeId = sHash[to_jsonSchemaURI]

						the_path = featureLoc.path
						if the_path.endswith("/" + ForeignKey.KeyAttributeNameFK):
							the_path = the_path[
								0 : -(len(ForeignKey.KeyAttributeNameFK) + 1)
							]

						mport = s_sum(schemaPath2JSONPath(the_path))

						fk_edge = FKEdge(
							fromNodeId=fromNodeId, mport=mport, toNodeId=toNodeId
						)
						fk_edges.append(fk_edge)
						fk_edges_d.setdefault(fromNodeId, []).append(fk_edge)
			elif the_id == PrimaryKey.KeyAttributeNamePK:
				# TO FINISH
				pk_set = set()
				for featureLoc in featureLocs:
					the_path = featureLoc.path
					if the_path.endswith("/" + PrimaryKey.KeyAttributeNamePK):
						the_path = the_path[
							0 : -(len(PrimaryKey.KeyAttributeNamePK) + 1)
						]
					json_path = schemaPath2JSONPath(the_path)
					if len(json_path) > 0:
						json_path += "."
					for pk_decl in featureLoc.context[PrimaryKey.KeyAttributeNamePK]:
						pk_path = json_path + pk_decl
						mport = s_sum(pk_path)
						pk_set.add(mport)

				pk_sets[fromNodeId] = pk_set

	# First pass
	for jsonSchemaURI, schemaObj in validSchemaDict.items():
		nodeId_o = sHash.get(jsonSchemaURI)

		if nodeId_o is not None:
			resolved_schema = schemaObj["resolved_schema"]

			headerName = jsonSchemaURI
			rSlash = headerName.rfind("/")
			if rSlash != -1:
				headerName = headerName[rSlash + 1 :]

			label = headerName

			label = genObjectNodes(
				headerName,
				resolved_schema,
				None,
				pk_set=pk_sets.get(nodeId_o),
				fk_edges=fk_edges_d.get(nodeId_o),
				schema_id=jsonSchemaURI,
			)

			if label is not None:
				DOT.write(f"\t{nodeId_o} [label=<\n{label}\n>];\n")

	# Second pass
	for fk_edge in fk_edges:
		customEdge = ""
		if fk_edge.fromNodeId == fk_edge.toNodeId:
			customEdge = " [headport=e]"

		DOT.write(
			f'\t{fk_edge.fromNodeId}:"{fk_edge.mport}" -> {fk_edge.toNodeId}:schema{customEdge};\n'
		)

	post = """
}
"""
	DOT.write(post)

	return 0
